package org.hucompute.textimager.uima.agreement.engine.serialization;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Streams;
import de.tudarmstadt.ukp.dkpro.core.api.metadata.type.DocumentMetaData;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Token;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.lang3.StringUtils;
import org.apache.uima.UimaContext;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.cas.CASException;
import org.apache.uima.fit.component.JCasAnnotator_ImplBase;
import org.apache.uima.fit.descriptor.ConfigurationParameter;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.apache.uima.jcas.cas.TOP;
import org.apache.uima.jcas.tcas.Annotation;
import org.apache.uima.resource.ResourceInitializationException;
import org.dkpro.core.api.parameter.ComponentParameters;
import org.hucompute.textimager.uima.agreement.engine.AbstractIAAEngine;
import org.texttechnologylab.annotation.AbstractNamedEntity;
import org.texttechnologylab.annotation.NamedEntity;
import org.texttechnologylab.annotation.type.Fingerprint;
import org.texttechnologylab.utilities.collections.IndexingMap;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;


public class CsvPrinterEngine extends JCasAnnotator_ImplBase {
	/**
	 * Output file path.
	 */
	public static final String PARAM_TARGET_LOCATION = ComponentParameters.PARAM_TARGET_LOCATION;
	@ConfigurationParameter(
			name = ComponentParameters.PARAM_TARGET_LOCATION,
			mandatory = true
	)
	private String targetLocation;
	
	/**
	 * If true, only consider annotations coverd by a {@link Fingerprint}.
	 */
	public static final String PARAM_FILTER_FINGERPRINTED = "pFilterFingerprinted";
	@ConfigurationParameter(
			name = PARAM_FILTER_FINGERPRINTED,
			defaultValue = "true"
	)
	private Boolean pFilterFingerprinted;
	
	/**
	 * The minimal number of views any given document has to have, to be printed.
	 */
	public static final String PARAM_MIN_VIEWS = "pMinViews";
	@ConfigurationParameter(
			name = PARAM_MIN_VIEWS,
			defaultValue = "2"
	)
	private Integer pMinViews;
	
	/**
	 * Defines the relation of the given annotators:
	 * <ul>
	 * <li>{@link AbstractIAAEngine#WHITELIST}: only the listed annotators will be considered.</li>
	 * <li>{@link AbstractIAAEngine#BLACKLIST}: all listed annotators will be excluded.</li>
	 * </ul>
	 */
	public static final String PARAM_ANNOTATOR_RELATION = "pRelation";
	@ConfigurationParameter(
			name = PARAM_ANNOTATOR_RELATION,
			defaultValue = "true",
			mandatory = false,
			description = "Decides weather to white- to or blacklist the given annotators."
	)
	Boolean pWhitelisting;
	public static final Boolean WHITELIST = true;
	public static final Boolean BLACKLIST = false;
	
	public static final String PARAM_ANNOTATOR_LIST = "pAnnotatorList";
	@ConfigurationParameter(name = PARAM_ANNOTATOR_LIST, mandatory = false)
	protected String[] pAnnotatorList;
	private ImmutableSet<String> listedAnnotators = ImmutableSet.of();
	
	private CSVPrinter csvPrinter;
	
	@Override
	public void initialize(UimaContext context) throws ResourceInitializationException {
		super.initialize(context);
		try {
			Path targetPath = Paths.get(targetLocation);
			if (!targetPath.toFile().exists())
				Files.createDirectories(targetPath.getParent());
			csvPrinter = new CSVPrinter(Files.newBufferedWriter(targetPath, StandardCharsets.UTF_8), CSVFormat.DEFAULT.withCommentMarker('#').withDelimiter(';'));
		} catch (IOException e) {
			throw new ResourceInitializationException(e);
		}
		
		if (pAnnotatorList != null && pAnnotatorList.length > 0) {
			listedAnnotators = ImmutableSet.copyOf(pAnnotatorList);
		}
	}
	
	@Override
	public void process(JCas jCas) throws AnalysisEngineProcessException {
		try {
			long views = Streams.stream(jCas.getViewIterator())
					.map(view -> StringUtils.substringAfterLast(view.getViewName().trim(), "/"))
					.filter(StringUtils::isNotEmpty)
					.count();
			
			if (views >= pMinViews) {
				HashMap<String, HashMap<Integer, ArrayList<String>>> viewHierarchyMap = new HashMap<>();
				LinkedHashSet<String> viewNames = new LinkedHashSet<>();
				
				jCas.getViewIterator().forEachRemaining(viewCas -> {
					// Split user id from view name and get annotator index for this id. Discards "_InitialView"
					String viewName = StringUtils.substringAfterLast(viewCas.getViewName().trim(), "/");
					// Check for empty view name and correct listing
					// If whitelisting (true), the name must be in the set; if blacklisting (false), it must not be in the set
					if (StringUtils.isEmpty(viewName) || pWhitelisting != listedAnnotators.contains(viewName))
						return;
					viewNames.add(viewName);
					
					HashMap<Integer, ArrayList<String>> neMap = new HashMap<>();
					IndexingMap<Token> tokenIndexingMap = new IndexingMap<>();
					ArrayList<Token> vTokens = new ArrayList<>(JCasUtil.select(viewCas, Token.class));
					vTokens.forEach(tokenIndexingMap::add);
					
					HashSet<TOP> fingerprinted = JCasUtil.select(viewCas, Fingerprint.class).stream()
							.distinct()
							.map(Fingerprint::getReference)
							.collect(Collectors.toCollection(HashSet::new));
					
					for (Class<? extends Annotation> type : Lists.newArrayList(NamedEntity.class, AbstractNamedEntity.class)) {
						Map<Annotation, Collection<Token>> neIndex = JCasUtil.indexCovered(viewCas, type, Token.class);
						for (Map.Entry<Annotation, Collection<Token>> entry : neIndex.entrySet()) {
							Annotation ne = entry.getKey();
							if (!pFilterFingerprinted || fingerprinted.contains(ne))
								for (Token token : entry.getValue()) {
									Integer index = tokenIndexingMap.get(token);
									ArrayList<String> nes = neMap.getOrDefault(index, new ArrayList<>());
									nes.add(ne.getType().getName().replaceFirst("org.texttechnologylab.annotation.type.", ""));
									neMap.put(index, nes);
								}
						}
					}
					
					for (int i = 0; i < vTokens.size(); i++) {
						if (!neMap.containsKey(i)) {
							neMap.put(i, Lists.newArrayList("O"));
						}
					}
					viewHierarchyMap.put(viewName, neMap);
				});
				
				try {
					Optional<DocumentMetaData> documentMetaData = Optional.ofNullable(DocumentMetaData.get(jCas));
					csvPrinter.printRecord(Lists.asList(String.format("#%s", documentMetaData.isPresent() ? documentMetaData.get().getDocumentId() : "NULL"), viewNames.toArray(new String[0])));
					
					ArrayList<Token> tokens = new ArrayList<>(JCasUtil.select(jCas, Token.class));
					for (int i = 0; i < tokens.size(); i++) {
						Token tToken = tokens.get(i);
						final Integer index = i;
						String[] annotations = viewNames.stream().map(name -> String.join(", ", viewHierarchyMap.get(name).get(index))).toArray(String[]::new);
						csvPrinter.printRecord(Lists.asList(tToken.getCoveredText(), annotations));
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		} catch (CASException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public void collectionProcessComplete() throws AnalysisEngineProcessException {
		super.collectionProcessComplete();
		try {
			csvPrinter.flush();
			csvPrinter.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
