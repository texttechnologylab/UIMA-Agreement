package biofid.engine.agreement;

import biofid.utility.CountMap;
import biofid.utility.IndexingMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import de.tudarmstadt.ukp.dkpro.core.api.metadata.type.DocumentMetaData;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Token;
import org.apache.commons.lang3.StringUtils;
import org.apache.uima.UimaContext;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.cas.CASException;
import org.apache.uima.fit.descriptor.ConfigurationParameter;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.apache.uima.jcas.cas.TOP;
import org.apache.uima.jcas.tcas.Annotation;
import org.apache.uima.resource.ResourceInitializationException;
import org.dkpro.statistics.agreement.IAgreementMeasure;
import org.dkpro.statistics.agreement.ICategorySpecificAgreement;
import org.dkpro.statistics.agreement.coding.*;
import org.dkpro.statistics.agreement.distance.NominalDistanceFunction;
import org.texttechnologielab.annotation.type.Fingerprint;
import org.texttechnologylab.iaa.Agreement;

import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.apache.uima.fit.util.JCasUtil.indexCovering;

/**
 * Inter-annotator agreement engine using a {@link CodingAnnotationStudy CodingAnnotationStudy} and
 * {@link ICategorySpecificAgreement ICategorySpecificAgreement} measure.
 * <p/>
 * Creates one {@link CodingAnnotationStudy CodingAnnotationStudy} in total for which the given agreement measure is computed.U
 * <p/>
 *
 * @see CohenKappaAgreement
 * @see FleissKappaAgreement
 * @see KrippendorffAlphaAgreement
 * @see PercentageAgreement
 */
public class CodingIAACollectionProcessingEngine extends AbstractIAAEngine {
	private TreeSet<String> categories = new TreeSet<>();
	private Integer maxCasIndex = 0;
	private HashMap<Integer, HashMap<String, HashMap<Integer, Set<String>>>> perCasStudies = new HashMap<>();
	private HashMap<Integer, Integer> perCasTokenCount = new HashMap<>();
	private LinkedHashSet<String> annotatorList = new LinkedHashSet<>();
	
	/**
	 * Parameter for the {@link SetSelectionStrategy SetSelectionStrategy} to use.<br>
	 * Default: {@link SetSelectionStrategy#MAX}.<br>
	 * Choices: <ul>
	 * <li>{@link SetSelectionStrategy#ALL}
	 * <li>{@link SetSelectionStrategy#MAX}
	 * </ul>
	 */
	public static final String PARAM_SET_SELECTION_STRATEGY = "pSetSelectionStrategy";
	@ConfigurationParameter(
			name = PARAM_SET_SELECTION_STRATEGY,
			defaultValue = "MAX",
			description = "Parameter for the SetSelectionStrategy to use."
	)
	private
	String pSetSelectionStrategy;
	
	/**
	 * Create per-token {@link Agreement} annotations in the given CAS.<br>
	 * The annotations will be added to a view with the name "IAA". If the view does not exist, it will be created.
	 * <p/>
	 * <b>IMPORTANT:</b>
	 * Requires chosen agreement measure to implement interface {@link ICodingItemSpecificAgreement}!
	 * <br>
	 * Currently supported measures:<ul>
	 * <li>{@link CodingIAACollectionProcessingEngine#KrippendorffAlphaAgreement CodingInterAnnotatorAgreementEngine.KrippendorffAlphaAgreement}
	 * <li>{@link CodingIAACollectionProcessingEngine#PercentageAgreement CodingInterAnnotatorAgreementEngine.PercentageAgreement}
	 * </ul>
	 */
	public static final String PARAM_ANNOTATE = "pAnnotate";
	@ConfigurationParameter(
			name = PARAM_ANNOTATE,
			defaultValue = "false",
			description = "Set true to enable token-level inter-annotator agreement annotation"
	)
	Boolean pAnnotate;
	
	// Agreement measure choices
	/**
	 * Paramter string for {@link CodingIAACollectionProcessingEngine#CohenKappaAgreement}.
	 *
	 * @see CohenKappaAgreement
	 */
	public final static String CohenKappaAgreement = "CohenKappaAgreement";
	
	/**
	 * Paramter string for {@link CodingIAACollectionProcessingEngine#FleissKappaAgreement}.
	 *
	 * @see FleissKappaAgreement
	 */
	public final static String FleissKappaAgreement = "FleissKappaAgreement";
	
	/**
	 * Paramter string for {@link CodingIAACollectionProcessingEngine#PercentageAgreement}.
	 *
	 * @see PercentageAgreement
	 */
	public final static String PercentageAgreement = "PercentageAgreement";
	
	/**
	 * Paramter string for {@link CodingIAACollectionProcessingEngine#KrippendorffAlphaAgreement}.
	 *
	 * @see KrippendorffAlphaAgreement
	 */
	public final static String KrippendorffAlphaAgreement = "KrippendorffAlphaAgreement";
	
	/**
	 * Parameter for the agreement measure the to use.<br>
	 * Default: {@link CodingIAACollectionProcessingEngine#KrippendorffAlphaAgreement CodingInterAnnotatorAgreementEngine.KrippendorffAlphaAgreement}.<br>
	 * Choices:
	 * <ul>
	 * <li>{@link CodingIAACollectionProcessingEngine#KrippendorffAlphaAgreement CodingInterAnnotatorAgreementEngine.KrippendorffAlphaAgreement}
	 * <li>{@link CodingIAACollectionProcessingEngine#FleissKappaAgreement CodingInterAnnotatorAgreementEngine.FleissKappaAgreement}
	 * <li>{@link CodingIAACollectionProcessingEngine#CohenKappaAgreement CodingInterAnnotatorAgreementEngine.CohenKappaAgreement}
	 * <li>{@link CodingIAACollectionProcessingEngine#PercentageAgreement CodingInterAnnotatorAgreementEngine.PercentageAgreement}
	 * </ul>
	 * <p>
	 *
	 * @see CohenKappaAgreement
	 * @see FleissKappaAgreement
	 * @see KrippendorffAlphaAgreement
	 * @see PercentageAgreement
	 */
	public static final String PARAM_AGREEMENT_MEASURE = "pAgreementMeasure";
	@ConfigurationParameter(
			name = PARAM_AGREEMENT_MEASURE,
			defaultValue = KrippendorffAlphaAgreement,
			description = "Parameter for the agreement measure the to use."
	)
	String pAgreementMeasure;
	
	@Override
	public void initialize(UimaContext context) throws ResourceInitializationException {
		super.initialize(context);
		if (pAnnotate && !(ImmutableSet.of(KrippendorffAlphaAgreement, PercentageAgreement).contains(pAgreementMeasure))) {
			throw new ResourceInitializationException(new UnsupportedOperationException(
					"PARAM_ANNOTATE is set 'true', but the chosen PARAM_AGREEMENT_MEASURE does not implement ICodingItemSpecificAgreement!"
			));
		}
	}
	
	@Override
	public void process(JCas jCas) throws AnalysisEngineProcessException {
		try {
			if (!isCasValid(jCas)) return;
			
			// Count all not sub-tokens
			int tokenCount = (int) JCasUtil.select(jCas, Token.class).stream()
					.filter(((Predicate<Token>)
							indexCovering(jCas, Token.class, Token.class).entrySet().stream()
									.filter(entry -> !entry.getValue().isEmpty())
									.map(Map.Entry::getKey)
									.collect(Collectors.toCollection(HashSet::new))::contains)
							.negate())
					.count();
			perCasTokenCount.put(maxCasIndex, tokenCount);
			
			// Count all annotations for PARAM_MIN_ANNOTATIONS
			CountMap<String> perViewAnnotationCount = new CountMap<>();
			
			// Iterate over all views
			HashMap<String, HashMap<Integer, Set<String>>> perViewAnnotationMap = new HashMap<>();
			for (String fullViewName : validViewNames) {
				JCas viewCas = jCas.getView(fullViewName);
				// Split user id from view name and get annotator index for this id. Discards "_InitialView"
				String viewName = StringUtils.substringAfterLast(fullViewName.trim(), "/");
				annotatorList.add(viewName);
				
				// Get all fingerprinted annotations
				HashSet<TOP> fingerprinted = JCasUtil.select(viewCas, Fingerprint.class).stream()
						.map(Fingerprint::getReference).collect(Collectors.toCollection(HashSet::new));
				
				// Create an index for the token, that are not part of sub-token
				HashSet<Token> coveredTokens = indexCovering(viewCas, Token.class, Token.class).entrySet().stream()
						.filter(entry -> !entry.getValue().isEmpty())
						.map(Map.Entry::getKey)
						.collect(Collectors.toCollection(HashSet::new));
				
				// Create an index for the tokens
				IndexingMap<Token> tokenIndexingMap = new IndexingMap<>();
				JCasUtil.select(viewCas, Token.class).stream()
						.sequential()
						.filter(((Predicate<Token>) coveredTokens::contains).negate())
						.forEachOrdered(tokenIndexingMap::add);
				
				if (tokenIndexingMap.size() != tokenCount) {
					logger.error("The number of tokens in this view does not match with the number of tokens in the default view!");
					return;
				}
				
				// Create a map which holds all annotation sets over all covered tokens (by index)
				HashMap<Integer, Set<String>> currentViewAnnotationMap = new HashMap<>();
				perViewAnnotationMap.put(viewName, currentViewAnnotationMap);
				
				// Add all annotations of each given class over each token to
				for (Class<? extends Annotation> annotationClass : annotationClasses) {
					Map<Token, Collection<Annotation>> annotationCoveringTokenIndex = indexCovering(viewCas, Token.class, annotationClass);
					for (Token token : tokenIndexingMap.keySet()) {
						if (!coveredTokens.contains(token)) {
							Integer index = tokenIndexingMap.get(token);
							for (Annotation annotation : annotationCoveringTokenIndex.get(token)) {
								// Check pFilterFingerprinted -> fingerprinted::contains
								if (!pFilterFingerprinted || fingerprinted.contains(annotation)) {
									String category = getCatgoryName(annotation);
									
									Set<String> categorySet = currentViewAnnotationMap.getOrDefault(index, new HashSet<>());
									categorySet.add(category);
									currentViewAnnotationMap.put(index, categorySet);
									
									perViewAnnotationCount.inc(viewName);
								}
							}
						}
					}
				}
			}
			
			// Check PARAM_MIN_ANNOTATIONS constraint
			long min = annotatorList.stream()
					.map(perViewAnnotationCount::get)
					.min(Long::compareTo).orElse(0L);
			if (min < pMinAnnotations)
				return;
			
			// After all views have been processed, add the perViewAnnotationMap to perCasStudies
			perCasStudies.put(maxCasIndex, perViewAnnotationMap); // FIXME: Refactor this with the token count into an object?
			
			// If pAggregationMethod is SEPARATE or BOTH, compute agreement for this CAS only
			switch (pMultiCasHandling) {
				case SEPARATE:
				case BOTH:
					handleSeparate(jCas, perCasTokenCount.get(maxCasIndex), perViewAnnotationMap);
					break;
			}
			maxCasIndex += 1;
		} catch (CASException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Create a global study from all items if {@link CodingIAACollectionProcessingEngine#PARAM_MULTI_CAS_HANDLING PARAM_MULTI_CAS_HANDLING}
	 * is either BOTH or COMBINED.
	 */
	@Override
	public void collectionProcessComplete() throws AnalysisEngineProcessException {
		if (annotatorList.size() > 1) {
			switch (pMultiCasHandling) {
				case SEPARATE:
					return;
				case BOTH:
				case COMBINED:
				default:
					handleCombined();
					break;
			}
		}
	}
	
	private void handleSeparate(JCas jCas, int tokenCount, HashMap<String, HashMap<Integer, Set<String>>> perCasStudy) {
		CountMap<String> globalCategoryCount = new CountMap<>();
		HashMap<String, CountMap<String>> annotatorCategoryCount = new HashMap<>();
		// Initialize a CountMap for each annotator
		for (String annotator : annotatorList) {
			annotatorCategoryCount.put(annotator, new CountMap<>());
		}
		
		// Per token lookup for the created annotation items
		LinkedHashMap<Integer, ICodingAnnotationItem[]> tokenItemLookup = new LinkedHashMap<>();
		
		SetCodingAnnotationStudy codingAnnotationStudy = new SetCodingAnnotationStudy(annotatorList.size(), SetSelectionStrategy.valueOf(pSetSelectionStrategy));
		CountMap<String> globalCategoryOverlap = new CountMap<>();
		for (int tokenIndex = 0; tokenIndex < tokenCount; tokenIndex++) {
			// Holds the sets of annotations for this token per annotator
			ArrayList<Set<String>> perTokenAnnotations = new ArrayList<>();
			CountMap<String> categoryOverlap = new CountMap<>();
			
			// Bool to check if any annotator has an annotation over the current token
			boolean any = false;
			
			// Get all annotations over the current token by index
			for (String annotatorName : annotatorList) {
				Set<String> category = perCasStudy
						.getOrDefault(annotatorName, new HashMap<>())
						.getOrDefault(tokenIndex, ImmutableSet.of(""));
				perTokenAnnotations.add(category);
				
				if (!category.contains("")) {
					any = true;
					categories.addAll(category);
					
					// Statistics
					globalCategoryCount.incAll(category);
					annotatorCategoryCount.get(annotatorName).incAll(category);
					categoryOverlap.incAll(category);
				}
			}
			if (any) {
				// Add the annotations to the study
				tokenItemLookup.put(tokenIndex, codingAnnotationStudy.addItemSetsAsArray(perTokenAnnotations.toArray(new Set[0])));
				
				// Increase the overlap count for each category with more than one vote
				categoryOverlap.forEach((o, integer) -> {
					if (integer > 1) globalCategoryOverlap.inc(o);
				});
			}
		}
		
		// Compute agreement
		IAgreementMeasure agreement = calcualteAgreement(codingAnnotationStudy, globalCategoryCount, annotatorCategoryCount, globalCategoryOverlap);
		
		if (pPrintStatistics) {
			// Print the agreement for all categories
			System.out.printf("\n%s - %s - %s\n" +
							"Inter-annotator agreement for %d annotators: %s\n" +
							"Category\tCount\tAgreement\n" +
							"Overall\t%d\t%f\n",
					pAgreementMeasure, pSetSelectionStrategy, DocumentMetaData.get(jCas).getDocumentTitle(),
					annotatorList.size(), annotatorList.toString(),
					codingAnnotationStudy.getUnitCount(), agreement.calculateAgreement());
			printStudyResultsAndStatistics((ICategorySpecificAgreement) agreement, globalCategoryCount, annotatorCategoryCount, categories, annotatorList);
			printCategoryOverlap(globalCategoryOverlap);
		}
		
		// If set, create per token annotations in the given JCas
		if (pAnnotate) {
			if (!(agreement instanceof ICodingItemSpecificAgreement))
				logger.error(String.format(""));
			createAgreementAnnotations(jCas, tokenItemLookup, (ICodingItemSpecificAgreement) agreement);
		}
	}
	
	private void createAgreementAnnotations(JCas jCas, LinkedHashMap<Integer, ICodingAnnotationItem[]> tokenItemLookup, ICodingItemSpecificAgreement agreement) {
		try {
			JCas viewIAA = JCasUtil.getView(jCas, "IAA", true);
			if (viewIAA.getDocumentText() == null)
				viewIAA.setDocumentText(jCas.getDocumentText());
			viewIAA.removeAllIncludingSubtypes(Agreement.type);
			
			// Iterate over all tokens that have an entry in
			LinkedList<Token> tokens = Lists.newLinkedList(JCasUtil.select(jCas, Token.class));
			for (Integer tokenIndex : tokenItemLookup.keySet()) {
				Token token = tokens.get(tokenIndex);
				ICodingAnnotationItem[] iCodingAnnotationItems = tokenItemLookup.get(tokenIndex);
				Double itemAgreementValue = Arrays.stream(iCodingAnnotationItems)
						.map(agreement::calculateItemAgreement)
						.reduce(Double::sum)
						.orElse(0.0);
				itemAgreementValue /= iCodingAnnotationItems.length;
				
				// Create agreement annotation and add to viewIAA indexes
				int begin = token.getBegin();
				int end = token.getEnd();
				Agreement itemAgreement = new Agreement(viewIAA, begin, end);
				itemAgreement.setAgreementValue(itemAgreementValue);
				itemAgreement.setAgreementMeasure(pAgreementMeasure);
				viewIAA.addFsToIndexes(itemAgreement);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private void handleCombined() {
		CountMap<String> globalCategoryCount = new CountMap<>();
		HashMap<String, CountMap<String>> annotatorCategoryCount = new HashMap<>();
		// Initialize a CountMap for each annotator
		for (String annotator : annotatorList) {
			annotatorCategoryCount.put(annotator, new CountMap<>());
		}
		
		SetCodingAnnotationStudy codingAnnotationStudy = new SetCodingAnnotationStudy(annotatorList.size(), SetSelectionStrategy.valueOf(pSetSelectionStrategy));
		CountMap<String> globalCategoryOverlap = new CountMap<>();
		for (int casIndex = 0; casIndex < maxCasIndex; casIndex++) {
			if (perCasStudies.containsKey(casIndex) && perCasTokenCount.containsKey(casIndex)) { // FIXME: remove this check, as it is unnecessary
				HashMap<String, HashMap<Integer, Set<String>>> perCasStudy = perCasStudies.get(casIndex);
				for (int tokenIndex = 0; tokenIndex < perCasTokenCount.get(casIndex); tokenIndex++) {
					// Holds the sets of annotations for this token per annotator
					ArrayList<Set<String>> perTokenAnnotations = new ArrayList<>();
					CountMap<String> categoryOverlap = new CountMap<>();
					
					// Bool to check if any annotator has an annotation over the current token
					boolean any = false;
					
					// Get all annotations over the current token by index
					for (String annotatorName : annotatorList) {
						Set<String> category = perCasStudy
								.getOrDefault(annotatorName, new HashMap<>())
								.getOrDefault(tokenIndex, ImmutableSet.of(""));
						perTokenAnnotations.add(category);
						
						if (!category.contains("")) {
							any = true;
							categories.addAll(category);
							
							// Statistics
							globalCategoryCount.incAll(category);
							annotatorCategoryCount.get(annotatorName).incAll(category);
							categoryOverlap.incAll(category);
						}
					}
					if (any) {
						// Add the annotations to the study
						codingAnnotationStudy.addItemSetsAsArray(perTokenAnnotations.toArray(new Set[0]));
						
						// Increase the overlap count for each category with more than one vote
						categoryOverlap.forEach((o, integer) -> {
							if (integer > 1) globalCategoryOverlap.inc(o);
						});
					}
				}
			}
		}
		
		// Compute agreement
		IAgreementMeasure agreement = calcualteAgreement(codingAnnotationStudy, globalCategoryCount, annotatorCategoryCount, globalCategoryOverlap);
		if (pPrintStatistics) {
			// Print the agreement for all categories
			System.out.printf("\n%s - %s\n" +
							"Inter-annotator agreement for %d annotators: %s\n" +
							"Category\tCount\tAgreement\n" +
							"Overall\t%d\t%f\n",
					pAgreementMeasure, pSetSelectionStrategy,
					annotatorList.size(), annotatorList.toString(),
					codingAnnotationStudy.getUnitCount(), agreement.calculateAgreement());
			printStudyResultsAndStatistics((ICategorySpecificAgreement) agreement, globalCategoryCount, annotatorCategoryCount, categories, annotatorList);
			printCategoryOverlap(globalCategoryOverlap);
		}
	}
	
	/**
	 * Calculate the agreement for the given study and print the resulting statistics.
	 *
	 * @param codingAnnotationStudy
	 * @param globalCategoryCount
	 * @param annotatorCategoryCount
	 * @param globalCategoryOverlap
	 */
	IAgreementMeasure calcualteAgreement(SetCodingAnnotationStudy codingAnnotationStudy, CountMap<String> globalCategoryCount, HashMap<String, CountMap<String>> annotatorCategoryCount, CountMap<String> globalCategoryOverlap) {
		// Choose the agreement measure method
		IAgreementMeasure agreement;
		switch (pAgreementMeasure) {
			case "CohenKappaAgreement":
				if (codingAnnotationStudy.getRaterCount() != 2) {
					throw new UnsupportedOperationException(String.format("CohenKappaAgreement only supports exactly 2 annotators, not %d!", codingAnnotationStudy.getRaterCount()));
				}
				agreement = new CohenKappaAgreement(codingAnnotationStudy);
				break;
			case "FleissKappaAgreement":
				agreement = new FleissKappaAgreement(codingAnnotationStudy);
				break;
			case "PercentageAgreement":
				agreement = new PercentageAgreement(codingAnnotationStudy);
				break;
			case "KrippendorffAlphaAgreement":
			default:
				agreement = new KrippendorffAlphaAgreement(codingAnnotationStudy, new NominalDistanceFunction());
				break;
		}
		
		return agreement;
	}
	
	private void printCategoryOverlap(CountMap<String> globalCategoryOverlap) {
		System.out.print("\nInter-annotator category overlap\nCategory\tCount\n");
		Optional<Long> totalOverlap = globalCategoryOverlap.values().stream().reduce(Long::sum);
		System.out.printf("Total\t%d\n", totalOverlap.orElse(0L));
		for (String category : categories) {
			System.out.printf("%s\t%d\n", category, globalCategoryOverlap.getOrDefault(category, 0L));
		}
	}
	
}
