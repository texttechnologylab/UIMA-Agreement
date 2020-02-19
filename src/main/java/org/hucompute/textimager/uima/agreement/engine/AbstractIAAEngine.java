package org.hucompute.textimager.uima.agreement.engine;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Streams;
import de.tudarmstadt.ukp.dkpro.core.api.parameter.ComponentParameters;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.lang3.StringUtils;
import org.apache.uima.UimaContext;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.cas.CASException;
import org.apache.uima.fit.component.JCasConsumer_ImplBase;
import org.apache.uima.fit.descriptor.ConfigurationParameter;
import org.apache.uima.fit.internal.ExtendedLogger;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.apache.uima.jcas.cas.DoubleArray;
import org.apache.uima.jcas.cas.LongArray;
import org.apache.uima.jcas.cas.StringArray;
import org.apache.uima.jcas.cas.TOP;
import org.apache.uima.jcas.tcas.Annotation;
import org.apache.uima.resource.ResourceInitializationException;
import org.dkpro.statistics.agreement.IAgreementMeasure;
import org.dkpro.statistics.agreement.ICategorySpecificAgreement;
import org.texttechnologielab.annotation.type.Fingerprint;
import org.texttechnologylab.iaa.Agreement;
import org.texttechnologylab.iaa.AgreementContainer;
import org.texttechnologylab.utilities.collections.CountMap;

import javax.annotation.Nonnull;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Abstract base class for all inter-annotator agreement engines.
 */
public abstract class AbstractIAAEngine extends JCasConsumer_ImplBase {

	/**
	 * An array of fully qualified names of classes, that extend {@link Annotation},
	 * which are to be considered in the agreement computation.
	 * Adding classes which extend other given classes might return unexpected results.
	 */
	public static final String PARAM_ANNOTATION_CLASSES = "pAnnotationClasses";
	protected CSVFormat csvFormat = CSVFormat.DEFAULT.withCommentMarker('#').withDelimiter(';');
	@ConfigurationParameter(name = PARAM_ANNOTATION_CLASSES, mandatory = false)
	private String[] pAnnotationClasses;
	ImmutableSet<Class<? extends Annotation>> annotationClasses = ImmutableSet.of(Annotation.class);

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
	Boolean pRelation;
	public static final Boolean WHITELIST = true;
	public static final Boolean BLACKLIST = false;

	public static final String PARAM_ANNOTATOR_LIST = "pAnnotatorList";
	@ConfigurationParameter(name = PARAM_ANNOTATOR_LIST, mandatory = false)
	private String[] pAnnotatorList;
	ImmutableSet<String> listedAnnotators = ImmutableSet.of();

	/**
	 * The minimal number of views in each CAS.
	 */
	public static final String PARAM_MIN_VIEWS = "pMinViews";
	@ConfigurationParameter(
			name = PARAM_MIN_VIEWS,
			mandatory = false,
			defaultValue = "2"
	)
	protected Integer pMinViews;

	/**
	 * The minimal number of eligible annotations in each view. Set to -1 to disable the constraint.
	 * <p/>
	 * Default: 10
	 */
	public static final String PARAM_MIN_ANNOTATIONS = "pMinAnnotations";
	@ConfigurationParameter(
			name = PARAM_MIN_ANNOTATIONS,
			mandatory = false,
			defaultValue = "10"
	)
	protected Integer pMinAnnotations;

	/**
	 * If true, only consider annotations coverd by a {@link Fingerprint}.
	 */
	public static final String PARAM_FILTER_FINGERPRINTED = "pFilterFingerprinted";
	@ConfigurationParameter(
			name = PARAM_FILTER_FINGERPRINTED,
			defaultValue = "true"
	)
	protected Boolean pFilterFingerprinted;

	/**
	 * If true, print agreement and annotation statistics.
	 * <br>
	 * Default: true.
	 */
	public static final String PARAM_PRINT_STATS = "pPrintStatistics";
	@ConfigurationParameter(
			name = PARAM_PRINT_STATS,
			mandatory = false,
			defaultValue = "true"
	)
	Boolean pPrintStatistics;

	/**
	 * Possible aggregation methods for the calculation of the inter-annotator agreement values:
	 * <ul>
	 *     <li>{@link AbstractIAAEngine#SEPARATE AbstractIAAEngine.SEPARATE}</li>
	 *     <li>{@link AbstractIAAEngine#COMBINED AbstractIAAEngine.COMBINED}</li>
	 *     <li>{@link AbstractIAAEngine#BOTH AbstractIAAEngine.BOTH}</li>
	 * </ul>
	 */
	public static final String PARAM_MULTI_CAS_HANDLING = "pMultiCasHandling";
	@ConfigurationParameter(
			name = PARAM_MULTI_CAS_HANDLING,
			mandatory = false,
			defaultValue = COMBINED
	)
	String pMultiCasHandling;

	/**
	 * {@link AbstractIAAEngine#PARAM_MULTI_CAS_HANDLING} choice. Process each of the CAS separately and output their
	 * inter-annotator agreement.
	 */
	public static final String SEPARATE = "SEPARATE";

	/**
	 * {@link AbstractIAAEngine#PARAM_MULTI_CAS_HANDLING} choice. Collect all annotations from each cas in a single study.
	 */
	public static final String COMBINED = "COMBINED";

	/**
	 * {@link AbstractIAAEngine#PARAM_MULTI_CAS_HANDLING} choice. Process each and collect them into a single study afterwards.
	 */
	public static final String BOTH = "BOTH";

	/**
	 * The path where statistic files will be created, if {@link AbstractIAAEngine#PARAM_PRINT_STATS} is set 'true'.<br>
	 * Will create one statistics file per input CAS if {@link AbstractIAAEngine#PARAM_MULTI_CAS_HANDLING} is
	 * {@link AbstractIAAEngine#SEPARATE} or {@link AbstractIAAEngine#BOTH} and one file for the combined statistics if
	 * {@link AbstractIAAEngine#PARAM_MULTI_CAS_HANDLING} is {@link AbstractIAAEngine#COMBINED} or
	 * {@link AbstractIAAEngine#BOTH}.
	 * <p/>
	 * This can also be set to {@link System#out} or {@link System#err}, in which case no files will be created but the
	 * output will be printed in the corresponding output stream.
	 * <p/>
	 * If the given path is an existing *.csv file, all statistics will then be appended to that file.
	 * If {@link AbstractIAAEngine#PARAM_OVERWRITE_EXISTING} is set 'true', the file will be truncated to zero length
	 * during the call of {@link AbstractIAAEngine#initialize}. If it does not exist, it will be created.
	 * <p/>
	 * If the given path does not exist, it will be created.
	 */
	public static final String PARAM_TARGET_LOCATION = ComponentParameters.PARAM_TARGET_LOCATION;
	@ConfigurationParameter(
			name = PARAM_TARGET_LOCATION,
			defaultValue = "System.out"
	)
	private String targetLocation;

	/**
	 * Whether to overwrite existing files in the given target location.
	 * If set false, statistics will be appended to existing files.
	 * <p/>
	 * Default: true.
	 */
	public static final String PARAM_OVERWRITE_EXISTING = "pOverwriteExisting";
	@ConfigurationParameter(
			name = PARAM_OVERWRITE_EXISTING,
			defaultValue = "true"
	)
	private Boolean pOverwriteExisting;

	public static final String PARAM_ANNOTATE_DOCUMENT = "pAnnotateDocument";
	@ConfigurationParameter(name = PARAM_ANNOTATE_DOCUMENT, defaultValue = "true", mandatory = false,
			description = "Set false to disable document level IAA annotations. Default value: true. Has no effect when " +
					"PARAM_MULTI_CAS_HANDLING is set to 'COMBINED'."
	)
	Boolean pAnnotateDocument;

	protected ExtendedLogger logger;
	long viewCount;
	LinkedHashSet<String> validViewNames;
	private CSVPrinter globalCsvPrinter;

	@Override
	public void initialize(UimaContext context) throws ResourceInitializationException {
		super.initialize(context);
		logger = getLogger();
		ArrayList<Class<? extends Annotation>> classArrayList = new ArrayList<>();

		// If class names were passed as parameters, update the annotationClasses set
		if (pAnnotationClasses != null && pAnnotationClasses.length > 0) {
			for (String pAnnotationClass : pAnnotationClasses) {
				// Get a class from its class name and cast it to Class<? extends Annotation>
				try {
					Class<?> aClass = Class.forName(pAnnotationClass);
					if (Annotation.class.isAssignableFrom(aClass)) {
						classArrayList.add((Class<? extends Annotation>) aClass);
					}
				} catch (ClassNotFoundException e) {
					logger.warn(e.getMessage());
				}
			}
			// If any class could be found, update the set
			if (classArrayList.size() > 0)
				annotationClasses = ImmutableSet.copyOf(classArrayList);
		}

		// Set the list of annotators
		if (pAnnotatorList != null && pAnnotatorList.length > 0) {
			listedAnnotators = ImmutableSet.copyOf(pAnnotatorList);
		}
		logger.info("Computing inter-annotator agreement for subclasses of " + annotationClasses.toString());
		if (!listedAnnotators.isEmpty()) {
			logger.info(String.format("%s annotators with ids: %s", pRelation ? "Whitelisting" : "Blacklisting", listedAnnotators.toString()));
		}


		if (!Arrays.asList("System.out", "System.err").contains(targetLocation)) {
			try {
				Path targetPath = Paths.get(targetLocation);
				if (targetPath.toFile().exists() && targetPath.toFile().isFile()) { // Check if the target path is an existing file and if it is, whether it should be overwritten.
					if (pOverwriteExisting) {
						BufferedWriter bufferedWriter = Files.newBufferedWriter(targetPath, StandardCharsets.UTF_8, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
						globalCsvPrinter = new CSVPrinter(bufferedWriter, csvFormat);
					} else {
						BufferedWriter bufferedWriter = Files.newBufferedWriter(targetPath, StandardCharsets.UTF_8, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
						globalCsvPrinter = new CSVPrinter(bufferedWriter, csvFormat);
					}
				} else if (!targetPath.toFile().exists() && targetPath.toString().endsWith(".csv")) { // Check if the target path denotes a file
					Files.createDirectories(targetPath.getParent());
					BufferedWriter bufferedWriter = Files.newBufferedWriter(targetPath, StandardCharsets.UTF_8, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
					globalCsvPrinter = new CSVPrinter(bufferedWriter, csvFormat);
				} else if (!targetPath.toFile().exists()) { // If it does denote a directory, create it if it does not exist
					Files.createDirectories(targetPath);
				}
			} catch (Exception e) {
				throw new ResourceInitializationException(e);
			}
		}
	}

	CSVPrinter getCsvPrinter(@Nonnull String suffix) throws IOException {
		Appendable targetAppendable;
		switch (targetLocation) {
			case "System.out":
				targetAppendable = System.out;
				break;
			case "System.err":
				targetAppendable = System.err;
				break;
			default:
				// If initialize created global printer, return it
				if (globalCsvPrinter != null) return globalCsvPrinter;

				Path path = Paths.get(targetLocation);
				Path appendablePath = Paths.get(path.toString(), suffix);
				if (!appendablePath.toFile().exists() || pOverwriteExisting) { // File does not exist or pOverwriteExisting is true.
					targetAppendable = Files.newBufferedWriter(appendablePath, StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
				} else { // File does exist and pOverwriteExisting is false.
					targetAppendable = Files.newBufferedWriter(appendablePath, StandardCharsets.UTF_8, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
				}
		}
		return new CSVPrinter(targetAppendable, csvFormat);
	}

	/**
	 * Create a set of annotations, that are overlapped by another annotation
	 *
	 * @param viewCas     The cas containing the annotations.
	 * @param aClass      The class of the overlapped annotations.
	 * @param annotations A collection of all annotations.
	 * @return A HashSet of all annotations that are overlapped by any other annotation.
	 */
	@Nonnull
	protected HashSet<Annotation> getOverlappedAnnotations(JCas viewCas, Class<? extends Annotation> aClass, Collection<? extends Annotation> annotations) {
		HashSet<Annotation> overlappedAnnotations = new HashSet<>();
		for (Annotation annotation : annotations) {
			JCasUtil.subiterate(viewCas, aClass, annotation, false, false).forEach(item -> {
				if (annotation.getType().equals(item.getType()))
					overlappedAnnotations.add(item);
			});
		}
		return overlappedAnnotations;
	}

	protected void printStudyResultsAndStatistics(ICategorySpecificAgreement agreement, CountMap<String> categoryCount, HashMap<String, CountMap<String>> annotatorCategoryCount, TreeSet<String> categories, Collection<String> annotators, CSVPrinter csvPrinter) throws IOException {
		for (String category : categories) {
			double value = agreement.calculateCategoryAgreement(category);
			csvPrinter.printRecord(category, categoryCount.get(category), Double.isNaN(value) ? 0.0 : value);
		}
		csvPrinter.println();

		// Print annotation statistics for each annotator and all categories
		csvPrinter.printComment("Annotation statistics:");
		csvPrinter.printRecord(Lists.asList("Annotator", annotators.toArray(new String[0])));

		String[] countArray = annotators
				.stream()
				.map(annotator -> annotatorCategoryCount.get(annotator)
						.values()
						.stream()
						.reduce(Long::sum).orElse(0L)
						.toString())
				.toArray(String[]::new);
		csvPrinter.printRecord(Lists.asList("Total", countArray));

		for (String category : categories) {
			countArray = annotators
					.stream()
					.map(annotator -> annotatorCategoryCount.get(annotator).get(category).toString())
					.toArray(String[]::new);
			csvPrinter.printRecord(Lists.asList(category, countArray));
		}
		csvPrinter.println();
	}

	protected String getCatgoryName(Annotation annotation) {
		return annotation.getType().getName();
	}

	protected boolean isCasValid(JCas jCas) throws CASException {
		// Ensure document has SOFA string
		if (jCas.getDocumentText() == null || jCas.getDocumentText().isEmpty())
			return false;


		// Check for empty view name and correct listing
		validViewNames = Streams.stream(jCas.getViewIterator())
				.map(JCas::getViewName)
				.filter(fullName -> {
					// If whitelisting (true), the name must be in the set; if blacklisting (false), it must not be in the set
					String viewName = StringUtils.substringAfterLast(fullName.trim(), "/");
					return StringUtils.isNotEmpty(viewName) && pRelation == listedAnnotators.contains(viewName);
				})
				.collect(Collectors.toCollection(LinkedHashSet::new));

		// Check for annotation count
		if (pMinAnnotations > 0) {
			for (String fullViewName : ImmutableSet.copyOf(validViewNames)) {
				JCas viewCas = jCas.getView(fullViewName);

				// Get all fingerprinted annotations
				HashSet<TOP> fingerprinted = JCasUtil.select(viewCas, Fingerprint.class).stream()
						.map(Fingerprint::getReference)
						.collect(Collectors.toCollection(HashSet::new));

				long totalAnnotations = 0L;
				for (Class<? extends Annotation> annotationClass : annotationClasses) {
					// Count total annotations
					totalAnnotations += getAnnotations(viewCas, fingerprinted, annotationClass).size();
				}

				// Remove views with insufficient annotation count
				if (totalAnnotations < pMinAnnotations) {
					logger.debug(String.format("Removing view %s because it has insufficient annoations: %d < %d", fullViewName, totalAnnotations, pMinAnnotations));
					validViewNames.remove(fullViewName);
				}
			}
		}

		viewCount = validViewNames.size();

		// TODO: comment.
		// If was set, ensure there are multiple views other than _InitialView
		if (viewCount < pMinViews)
			return false;
		return true;
	}

	@Nonnull
	ArrayList<? extends Annotation> getAnnotations(JCas viewCas, HashSet<TOP> fingerprinted, Class<? extends Annotation> annotationClass) {
		ArrayList<? extends Annotation> annotations;
		if (pFilterFingerprinted)
			annotations = JCasUtil.select(viewCas, annotationClass).stream()
					.filter((Predicate<TOP>) fingerprinted::contains)
					.collect(Collectors.toCollection(ArrayList::new));
		else
			annotations = new ArrayList<>(JCasUtil.select(viewCas, annotationClass));

		// Create a set of annotations, that are overlapped by another annotation
		HashSet<Annotation> overlappedAnnotations = getOverlappedAnnotations(viewCas, annotationClass, annotations);
		// Remove annotations, that are overlapped by an annotation of the same Type
		annotations.removeAll(overlappedAnnotations);
		return annotations;
	}

	@Override
	public void collectionProcessComplete() throws AnalysisEngineProcessException {
		super.collectionProcessComplete();
	}

	@Override
	public void destroy() {
		super.destroy();
		if (globalCsvPrinter != null) {
			try {
				globalCsvPrinter.flush();
				globalCsvPrinter.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	@Nonnull
	protected JCas createDocumentAgreementAnnotations(JCas viewIAA, IAgreementMeasure agreement, String pAgreementMeasure, Set<String> categories, CountMap<String> globalCategoryCount) {
		AgreementContainer agreementContainer = new AgreementContainer(viewIAA);
		agreementContainer.setAgreementMeasure(pAgreementMeasure);
		agreementContainer.setOverallAgreementValue(agreement.calculateAgreement());

		String[] categoryStrings = categories.toArray(new String[0]);
		StringArray categoryNamesStringArray = new StringArray(viewIAA, categories.size());
		LongArray categoryCountsLongArray = new LongArray(viewIAA, categories.size());
		DoubleArray categoryValuesDoubleArray = new DoubleArray(viewIAA, categories.size());
		for (int i = 0; i < categoryStrings.length; i++) {
			String category = categoryStrings[i];
			double value = ((ICategorySpecificAgreement) agreement).calculateCategoryAgreement(category);
			categoryNamesStringArray.set(i, category);
			categoryValuesDoubleArray.set(i, Double.isNaN(value) ? 0.0 : value);
			categoryCountsLongArray.set(i, globalCategoryCount.getOrDefault(category, 0L));
		}
		agreementContainer.setCategoryNames(categoryNamesStringArray);
		agreementContainer.setCategoryAgreementValues(categoryValuesDoubleArray);
		agreementContainer.setCategoryCounts(categoryCountsLongArray);
		viewIAA.addFsToIndexes(agreementContainer);

		return viewIAA;
	}

	@Nonnull
	protected JCas initializeIaaView(JCas jCas) {
		JCas viewIAA = JCasUtil.getView(jCas, "IAA", true);
		if (viewIAA.getDocumentText() == null)
			viewIAA.setDocumentText(jCas.getDocumentText());
		viewIAA.removeAllIncludingSubtypes(Agreement.type);
		viewIAA.removeAllIncludingSubtypes(AgreementContainer.type);
		return viewIAA;
	}
}
