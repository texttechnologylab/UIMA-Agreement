package biofid.engine.agreement;

import biofid.utility.CountMap;
import biofid.utility.IndexingMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import de.tudarmstadt.ukp.dkpro.core.api.metadata.type.DocumentMetaData;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Token;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.cas.CASException;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.apache.uima.jcas.cas.TOP;
import org.apache.uima.jcas.tcas.Annotation;
import org.dkpro.statistics.agreement.unitizing.IUnitizingAnnotationUnit;
import org.dkpro.statistics.agreement.unitizing.KrippendorffAlphaUnitizingAgreement;
import org.dkpro.statistics.agreement.unitizing.UnitizingAnnotationStudy;
import org.texttechnologielab.annotation.type.Fingerprint;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.apache.uima.fit.util.JCasUtil.indexCovering;


/**
 * Inter-annotator agreement engine using {@link UnitizingAnnotationStudy UnitizingAnnotationStudies} and
 * {@link KrippendorffAlphaUnitizingAgreement KrippendorffAlphaUnitizingAgreement}.
 * <p/>
 * Creates one <i>local</i> {@link UnitizingAnnotationStudy UnitizingAnnotationStudy} for each CAS to be processed and
 * concatenates the results in a single <i>global</i> study for which the Krippendorff-Alpha-Agreement is computed.
 * <p/>
 *
 * @see KrippendorffAlphaUnitizingAgreement
 */
public class UnitizingIAACollectionProcessingEngine extends AbstractIAAEngine {
	
	private TreeSet<String> categories = new TreeSet<>();
	private AtomicInteger documentOffset = new AtomicInteger(0);
	private ArrayList<ImmutablePair<Integer, Iterable<IUnitizingAnnotationUnit>>> annotationStudies = new ArrayList<>();
	private IndexingMap<String> annotatorIndex = new IndexingMap<>();
	
	
	@Override
	public void process(JCas jCas) throws AnalysisEngineProcessException {
		try {
			if (!isCasValid(jCas)) return;
			
			// Initialize study
			int documentLength = JCasUtil.select(jCas, Token.class).size();
			UnitizingAnnotationStudy perCasStudy = new UnitizingAnnotationStudy((int) viewCount, documentLength);
			
			// Count all annotations for PARAM_MIN_ANNOTATIONS
			CountMap<String> perViewAnnotationCount = new CountMap<>();
			
			// Iterate over all views
			for (String fullViewName : validViewNames) {
				JCas viewCas = jCas.getView(fullViewName);
				// Split user id from view name and get annotator index for this id. Discards "_InitialView"
				String viewName = StringUtils.substringAfterLast(fullViewName.trim(), "/");
				annotatorIndex.add(viewName);
				
				// Get all fingerprinted annotations
				HashSet<TOP> fingerprinted = JCasUtil.select(viewCas, Fingerprint.class).stream()
						.map(Fingerprint::getReference)
						.collect(Collectors.toCollection(HashSet::new));
				
				// Create a set of all multi-tokens, that are covering another token
				HashSet<Token> coveredTokens = JCasUtil.indexCovering(viewCas, Token.class, Token.class).entrySet().stream()
						.filter(tokensCoveredByThisOne -> tokensCoveredByThisOne.getValue().size() > 1)
						.map(Map.Entry::getKey)
						.collect(Collectors.toCollection(HashSet::new));
				
				// Create an index for the token, that are not part of sub-token
				IndexingMap<Token> tokenIndexingMap = new IndexingMap<>();
				JCasUtil.select(viewCas, Token.class).stream()
						.filter(((Predicate<Token>) coveredTokens::contains).negate())
						.forEachOrdered(tokenIndexingMap::add);
				
				// Select all annotations of all given types and add an annotation unit for each item
				for (Class<? extends Annotation> annotationClass : annotationClasses) {
					ArrayList<? extends Annotation> annotations;
					if (pFilterFingerprinted)
						annotations = JCasUtil.select(viewCas, annotationClass).stream()
								.filter((Predicate<TOP>) fingerprinted::contains)
								.collect(Collectors.toCollection(ArrayList::new));
					else
						annotations = new ArrayList<>(JCasUtil.select(viewCas, annotationClass));
					
					HashMap<? extends Annotation, Collection<Token>> annotationTokenLookup = Maps.newHashMap(indexCovering(viewCas, annotationClass, Token.class));
					
					// Create a set of annotations, that are overlapped by another annotation
					HashSet<Annotation> overlappedAnnotations = getOverlappedAnnotations(viewCas, annotationClass, annotations);
					// Remove annotations, that are overlapped by an annotation of the same Type
					annotations.removeAll(overlappedAnnotations);
					
					for (Annotation annotation : annotations) {
						LinkedHashSet<Token> containedTokens = Sets.newLinkedHashSet(JCasUtil.subiterate(viewCas, Token.class, annotation, true, true));
						containedTokens.addAll(annotationTokenLookup.getOrDefault(annotation, Sets.newLinkedHashSet()));
						
						// initialize indexes
						int begin = Integer.MAX_VALUE;
						int end = Integer.MIN_VALUE;
						
						// Check if the token is indexed
						// If not, it is part of a sub-token and will be skipped
						containedTokens.retainAll(tokenIndexingMap.keySet());
						if (containedTokens.isEmpty())
							continue;
						
						for (Token token : containedTokens) {
							if (!tokenIndexingMap.containsKey(token))
								continue;
							
							int index = tokenIndexingMap.get(token);
							if (index < begin) {
								begin = index;
							}
							if (index > end) {
								end = index;
							}
						}
						
						if (end == Integer.MIN_VALUE || begin == Integer.MAX_VALUE) {
							logger.error("Error during annotation boundary detection!");
							continue;
						}
						
						String category = getCatgoryName(annotation);
						int length = end - begin + 1;
						perCasStudy.addUnit(
								begin,
								length,
								annotatorIndex.get(viewName),
								category
						);
						categories.add(category);
						
						perViewAnnotationCount.inc(viewName);
					}
				}
			}
			
			// Check PARAM_MIN_ANNOTATIONS constraint
			long min = annotatorIndex.keySet().stream()
					.map(perViewAnnotationCount::get)
					.min(Long::compareTo).orElse(0L);
			if (min < pMinAnnotations)
				return;
			
			// Store the collected annotations units and update the document offset for final evaluation
			annotationStudies.add(ImmutablePair.of(documentOffset.get(), perCasStudy.getUnits()));
			documentOffset.getAndAdd(documentLength);
			
			switch (pMultiCasHandling) {
				case SEPARATE:
				case BOTH:
					handleSeparate(jCas, perCasStudy);
					break;
			}
		} catch (CASException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public void collectionProcessComplete() throws AnalysisEngineProcessException {
		super.collectionProcessComplete();
		if (annotatorIndex.size() > 1) {
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
	
	private void handleSeparate(JCas jCas, UnitizingAnnotationStudy completeStudy) {
		// Iterate over all previously collected studies
		CountMap<String> categoryCount = new CountMap<>();
		HashMap<String, CountMap<String>> annotatorCategoryCount = new HashMap<>();
		
		// Initialize a CountMap for each annotator
		for (String annotator : annotatorIndex.keySet()) {
			annotatorCategoryCount.put(annotator, new CountMap<>());
		}
		
		for (IUnitizingAnnotationUnit annotationUnit : completeStudy.getUnits()) {
			int id = annotationUnit.getRaterIdx();
			String category = (String) annotationUnit.getCategory();
			
			// Update category counts
			categoryCount.inc(category);
			annotatorCategoryCount.get(annotatorIndex.getKey(id)).inc(category);
		}
		
		// Compute and print the agreement for all categories
		KrippendorffAlphaUnitizingAgreement agreement = new KrippendorffAlphaUnitizingAgreement(completeStudy);
		System.out.printf("\nKrippendorffAlphaUnitizingAgreement - %s\n" +
						"Inter-annotator agreement for %d annotators: %s\n" +
						"Category\tCount\tAgreement\n" +
						"Overall\t%d\t%f\n",
				DocumentMetaData.get(jCas).getDocumentTitle(),
				annotatorIndex.size(), annotatorIndex.keySet().toString(),
				completeStudy.getUnitCount(), agreement.calculateAgreement());
		printStudyResultsAndStatistics(agreement, categoryCount, annotatorCategoryCount, categories, annotatorIndex.keySet());
	}
	
	private void handleCombined() {
		UnitizingAnnotationStudy completeStudy = new UnitizingAnnotationStudy(annotatorIndex.size(), documentOffset.get());
		CountMap<String> categoryCount = new CountMap<>();
		HashMap<String, CountMap<String>> annotatorCategoryCount = new HashMap<>();
		
		// Initialize a CountMap for each annotator
		for (String annotator : annotatorIndex.keySet()) {
			annotatorCategoryCount.put(annotator, new CountMap<>());
		}
		
		// Iterate over all previously collected studies
		for (ImmutablePair<Integer, Iterable<IUnitizingAnnotationUnit>> study : annotationStudies) {
			int studyOffset = study.getLeft();
			
			// Add all annotation units from the study with correct offset
			for (IUnitizingAnnotationUnit annotationUnit : study.getRight()) {
				int id = annotationUnit.getRaterIdx();
				long offset = annotationUnit.getOffset();
				long length = annotationUnit.getLength();
				String category = (String) annotationUnit.getCategory();
				
				completeStudy.addUnit(studyOffset + offset, length, id, category);
				
				// Update category counts
				categoryCount.inc(category);
				annotatorCategoryCount.get(annotatorIndex.getKey(id)).inc(category);
			}
		}
		
		// Compute and print the agreement for all categories
		KrippendorffAlphaUnitizingAgreement agreement = new KrippendorffAlphaUnitizingAgreement(completeStudy);
		System.out.printf("\nKrippendorffAlphaUnitizingAgreement\nInter-annotator agreement for %d annotators: %s\n" +
						"Category\tCount\tAgreement\n" +
						"Overall\t%d\t%f\n",
				annotatorIndex.size(), annotatorIndex.keySet().toString(), completeStudy.getUnitCount(), agreement.calculateAgreement());
		printStudyResultsAndStatistics(agreement, categoryCount, annotatorCategoryCount, categories, annotatorIndex.keySet());
	}
	
}
