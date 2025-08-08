package org.hucompute.textimager.uima.agreement.engine.ttlab;

import com.google.common.collect.Sets;
import de.tudarmstadt.ukp.dkpro.core.api.metadata.type.DocumentMetaData;
import eu.openminted.share.annotations.api.Component;
import eu.openminted.share.annotations.api.constants.OperationType;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.HttpResponseException;
import org.apache.uima.UimaContext;
import org.apache.uima.cas.CAS;
import org.apache.uima.cas.impl.XmiCasDeserializer;
import org.apache.uima.cas.impl.XmiCasSerializer;
import org.apache.uima.cas.impl.XmiSerializationSharedData;
import org.apache.uima.collection.CollectionException;
import org.apache.uima.fit.component.CasCollectionReader_ImplBase;
import org.apache.uima.fit.descriptor.ConfigurationParameter;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.util.CasIOUtils;
import org.apache.uima.util.Logger;
import org.apache.uima.util.Progress;
import org.apache.uima.util.ProgressImpl;
import org.dkpro.core.api.parameter.ComponentParameters;
import org.json.JSONArray;
import org.json.JSONObject;
import org.texttechnologylab.utilities.helper.RESTUtils;

import java.io.*;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static de.tudarmstadt.ukp.dkpro.core.api.metadata.type.DocumentMetaData.create;
import static de.tudarmstadt.ukp.dkpro.core.api.metadata.type.DocumentMetaData.get;

// NOTE taken from "Utilities" where it got removed
// https://github.com/texttechnologylab/Utilities/blob/35f180bfc1758c5a943614b6aaaf2815ac726b2e/src/main/java/org/texttechnologylab/utilities/uima/reader/TextAnnotatorRepositoryCollectionReader.java
@Component(value = OperationType.READER)
public class TextAnnotatorRepositoryCollectionReader extends CasCollectionReader_ImplBase {
	private Logger logger;

	public static final String PARAM_TEXT_ANNOTATOR_URL = "pTextAnnotatorUrl";
	@ConfigurationParameter(
			name = PARAM_TEXT_ANNOTATOR_URL,
			mandatory = false,
			defaultValue = "http://api.textannotator.texttechnologylab.org:80/"
	)
	private static String pTextAnnotatorUrl;

	public static final String PARAM_MONGO_DB_URL = "pMongoDb";
	@ConfigurationParameter(
			name = PARAM_MONGO_DB_URL,
			mandatory = false,
			defaultValue = "https://resources.hucompute.org/mongo/"
	)
	private static String pMongoDb;

	public static final String PARAM_REMOTE_THREADS = "pThreads";
	@ConfigurationParameter(
			name = PARAM_REMOTE_THREADS,
			mandatory = false,
			defaultValue = "8"
	)
	private static int pThreads;

	public static final String PARAM_DOCUMENTS_REPOSITORY = "pRepository";
	@ConfigurationParameter(
			name = PARAM_DOCUMENTS_REPOSITORY,
			mandatory = false,
			defaultValue = "19147"  // Annotation neues Schema
//			defaultValue = "14393"  // Delivery 1 + 2
	)
	private static String pRepository;

	public static final String PARAM_SESSION_ID = "pSessionId";
	@ConfigurationParameter(
			name = PARAM_SESSION_ID
	)
	private static String pSessionId;

	public static final String PARAM_SOURCE_LOCATION = ComponentParameters.PARAM_SOURCE_LOCATION;
	@ConfigurationParameter(
			name = ComponentParameters.PARAM_SOURCE_LOCATION
	)
	private static String sourceLocation;

	public static final String PARAM_TARGET_LOCATION = ComponentParameters.PARAM_TARGET_LOCATION;
	@ConfigurationParameter(
			name = ComponentParameters.PARAM_TARGET_LOCATION,
			mandatory = false,
			defaultValue = ""
	)
	private static String targetLocation;

	public static final String PARAM_FORCE_RESERIALIZE = "pForceReserialize";
	@ConfigurationParameter(
			name = PARAM_FORCE_RESERIALIZE,
			mandatory = false,
			defaultValue = "true"
	)
	private static Boolean pForceReserialize;

	/**
	 * The frequency with which read documents are logged. Default: 1 (log every document).
	 * <p>
	 * Set to 0 or negative values to deactivate logging.
	 */
	public static final String PARAM_LOG_FREQ = "logFreq";
	@ConfigurationParameter(name = PARAM_LOG_FREQ, mandatory = true, defaultValue = "1")
	private int logFreq;

	private static final ArrayDeque<Path> currentResources = new ArrayDeque<>();
	private static final HashSet<Future<Path>> processedResources = Sets.newHashSet();
	private ExecutorService remotePool;
	private AtomicInteger downloadCount = new AtomicInteger(0);
	private AtomicInteger processingCount = new AtomicInteger(0);
	private int totalDocumentCount = 0;
	private ConcurrentHashMap<Path, XmiSerializationSharedData> xmiSerializationSharedDataMap;

	//	private ProgressMeter downloadProgress;
//	private ProgressMeter processingProgress;
	private ArrayList<Future<Path>> tasks = new ArrayList<>();

	@Override
	public void initialize(UimaContext aContext) throws ResourceInitializationException {
		super.initialize(aContext);
		logger = getLogger();

		remotePool = Executors.newFixedThreadPool(pThreads);
		pSessionId = StringUtils.appendIfMissing(pSessionId, ".jvm1");

		String remoteFilesURL = pTextAnnotatorUrl + "documents/" + pRepository;
		logger.info(String.format("Fetching file URIs from %s", remoteFilesURL));
		final JSONObject remoteFiles = RESTUtils.getObjectFromRest(remoteFilesURL, pSessionId);

		if (remoteFiles.optBoolean("success")) {
			final JSONArray rArray = remoteFiles.optJSONArray("result");
			final ArrayList<String> remoteUris = IntStream.range(0, rArray.length())
					.map(rArray::optInt)
					.distinct()
					.sorted()
					.mapToObj(Objects::toString)
					.collect(Collectors.toCollection(ArrayList::new));
			totalDocumentCount = remoteUris.size();
//				downloadProgress = new ProgressMeter(totalDocumentCount);
//				processingProgress = new ProgressMeter(totalDocumentCount);

			logger.info(String.format("Downloading %d files in parallel with %d threads for from repository '%s'\nURIs: %s",
					totalDocumentCount, pThreads, pRepository, remoteUris.toString()));

			// Download and pre-process all remote files in parallel
			xmiSerializationSharedDataMap = new ConcurrentHashMap<>();
			for (String uri : remoteUris) {
				tasks.add(remotePool.submit(new DownloadCallable(uri)));
			}
		} else {
			logger.error("Repository URIs could not be fetched!");
			logger.error(remoteFiles.toString());
		}
	}

	@Override
	public void getNext(CAS aCAS) throws IOException, CollectionException {
		waitForNext();
		Path path = currentResources.pop();
		try (FileInputStream inputStream = FileUtils.openInputStream(path.toFile())) {
			XmiCasDeserializer.deserialize(inputStream, aCAS, true, xmiSerializationSharedDataMap.get(path));
		} catch (Exception e) {
			logger.error("Error while opening file: " + path.toString() + "\n" + e.getMessage());
		} finally {
			// Update process
			int processed = processingCount.incrementAndGet();
			Progress[] progressArray = this.getProgress();
			if (progressArray.length > 0) {
				Progress progress = progressArray[0];
				if (logFreq > 0 && processed % logFreq == 0) {
					logger.info(String.format("Processing %s, %s", progress.toString(), path.getFileName().toString()));
				}
			}
		}
	}

	@Override
	public void destroy() {
		super.destroy();
		if (remotePool != null)
			remotePool.shutdownNow();
	}

	@Override
	public boolean hasNext() throws IOException, CollectionException {
		waitForNext();
		return !currentResources.isEmpty();
	}

	public boolean allTasksDone() {
		return tasks.stream()
				.map(this::checkFuture)
				.reduce(Boolean::logicalAnd).orElse(true);
	}

	private void waitForNext() {
		try {
			synchronized (currentResources) {
				while (currentResources.isEmpty() && !this.allTasksDone()) {
					currentResources.wait(500);
				}
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	private Boolean checkFuture(Future<Path> future) {
		if (future.isDone()) {
			if (!processedResources.contains(future)) {
				processedResources.add(future);
				try {
					Path path = future.get();
					if (path != null) {
						currentResources.add(path);
					}
				} catch (InterruptedException | ExecutionException e) {
					e.printStackTrace();
				}
			}
			return true;
		} else {
			return false;
		}
	}

	@Override
	public Progress[] getProgress() {
		return new Progress[]{
				new ProgressImpl(downloadCount.get(), totalDocumentCount, "file")
		};
	}

	private class DownloadCallable implements Callable<Path> {
		final String uri;

		DownloadCallable(String uri) {
			this.uri = uri;
		}

		@Override
		public Path call() {
			String mongoUri = pMongoDb + uri;
			Path utf8Path = null;
			try {
				// Get document JSON from MongoDB
				JSONObject documentJSON = RESTUtils.getObjectFromRest(mongoUri, pSessionId);
				if (documentJSON.getBoolean("success")) {
					JCas jCas = JCasFactory.createJCas();
					String documentName = documentJSON.getJSONObject("result").getString("name");
					String documentId = documentName
							.replaceFirst("[^_]*_(\\d+)_.*", "$1")
							.replaceAll("\\.[^.]+$", "");

					// Download file
					URL casURL = new URL(pTextAnnotatorUrl + "cas/" + uri + "?session=" + pSessionId);
					utf8Path = Paths.get(sourceLocation, documentName);
					File utf8File = utf8Path.toFile();
					try {
						logger.debug(String.format("Downloading file %s..", casURL.toString()));
						FileUtils.copyInputStreamToFile(casURL.openStream(), utf8File);
						logger.debug(String.format("Downloaded file %s.", casURL.toString()));
					} catch (Exception e) {
						logger.warn("Could not copy file from input stream: " + casURL.toString());
						logger.warn(e.getMessage());
						// Delete old file
						delete(utf8File);
						updateDownloadProgress(mongoUri);
						return null; // FIXME
					}

					// Reserialize the UTF-8 forced JCas
					if (pForceReserialize) {
						try (FileInputStream inputStream = FileUtils.openInputStream(utf8File)) {
							CasIOUtils.load(inputStream, null, jCas.getCas(), true);
						} catch (Exception e) {
							logger.warn("Could not load file: " + utf8Path);
							logger.warn(e.getMessage());
							updateDownloadProgress(mongoUri);
							return null; // FIXME
						}

						if (JCasUtil.select(jCas, DocumentMetaData.class).size() == 0) {
							DocumentMetaData documentMetaData = create(jCas);
							documentMetaData.setDocumentId(documentId);
							documentMetaData.setDocumentUri(mongoUri);
							documentMetaData.setDocumentTitle(documentName);
//							jCas.addFsToIndexes(documentMetaData); // FIXME: is this still needed?
						} else {
							DocumentMetaData documentMetaData = get(jCas);
							if (documentMetaData.getDocumentUri() == null)
								documentMetaData.setDocumentUri(mongoUri);
							if (documentMetaData.getDocumentId() == null)
								documentMetaData.setDocumentId(documentId);
							if (documentMetaData.getDocumentTitle() == null)
								documentMetaData.setDocumentTitle(documentName);
						}

						// Delete old file
						delete(utf8File);

						// Reserialize file as xmi
						try (FileOutputStream outputStream = new FileOutputStream(utf8File)) {
							XmiSerializationSharedData xmiSerializationSharedData = new XmiSerializationSharedData();
							XmiCasSerializer.serialize(jCas.getCas(), null, outputStream, true, xmiSerializationSharedData);
							xmiSerializationSharedDataMap.put(utf8Path, xmiSerializationSharedData);
						} catch (Exception e) {
							logger.warn("Could not write reserialized file: " + utf8Path);
							logger.warn(e.getMessage());
							updateDownloadProgress(mongoUri);
							return null; // FIXME
						}
					}

					if (targetLocation != null && !targetLocation.equals("")) {
						Path textPath = Paths.get(targetLocation, uri + ".txt");
						try {
							// Write raw text as UTF-8 file
							if (jCas.getDocumentText() != null && !jCas.getDocumentText().isEmpty()) {
								String content = jCas.getDocumentText();
								try (PrintWriter pw = new PrintWriter(new OutputStreamWriter(Files.newOutputStream(textPath), StandardCharsets.UTF_8))) {
									pw.print(content);
								}
							}
						} catch (Exception e) {
							logger.warn("Could not write text file: " + textPath);
							logger.warn(e.getMessage());
							updateDownloadProgress(mongoUri);
							return null; // FIXME
						}
					}
				} else {
					// If response JSON misses the "success" field, throw an exception
					throw new HttpResponseException(400, String.format("Request to '%s' failed! Response: %s", mongoUri, documentJSON.toString()));
				}
			} catch (HttpResponseException httpE) {
				logger.error(httpE.getMessage());
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				updateDownloadProgress(mongoUri);
			}
			return utf8Path;
		}

		private void delete(File utf8File) throws IOException {
			// Delete old file
			try {
				FileUtils.forceDelete(utf8File);
			} catch (FileNotFoundException | NullPointerException e) {
				logger.warn("File " + utf8File.getPath() + " up for deletion could not be found.");
			}
		}
	}

	private void updateDownloadProgress(String mongoUri) {
		int downloads = downloadCount.incrementAndGet();
//				downloadProgress.setDone(downloads);
		if (logFreq > 0 && downloads % logFreq == 0) {
			logger.info(String.format("Download %s/%d, %s", downloads, totalDocumentCount, mongoUri));
		}
	}
}
