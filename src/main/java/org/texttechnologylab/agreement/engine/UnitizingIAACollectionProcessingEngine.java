package org.texttechnologylab.agreement.engine;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import de.tudarmstadt.ukp.dkpro.core.api.metadata.type.DocumentMetaData;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Token;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.cas.CASException;
import org.apache.uima.UimaContext;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.apache.uima.jcas.cas.TOP;
import org.apache.uima.jcas.tcas.Annotation;
import org.dkpro.statistics.agreement.unitizing.IUnitizingAnnotationUnit;
import org.dkpro.statistics.agreement.unitizing.KrippendorffAlphaUnitizingAgreement;
import org.dkpro.statistics.agreement.unitizing.UnitizingAnnotationStudy;
import org.dkpro.statistics.agreement.visualization.CoincidenceMatrixPrinter; 
import org.texttechnologylab.annotation.type.Fingerprint;
import org.texttechnologylab.utilities.collections.CountMap;
import org.texttechnologylab.utilities.collections.IndexingMap;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.apache.uima.fit.util.JCasUtil.indexCovering;


/**
 * Inter-annotator agreement engine using {@link UnitizingAnnotationStudy UnitizingAnnotationStudies} and {@link
 * KrippendorffAlphaUnitizingAgreement KrippendorffAlphaUnitizingAgreement}.
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
    private IndexingMap<String> globalAnnotatorIndex = new IndexingMap<>();

    private boolean bDoHandleCombined;
    private boolean bDoHandleSeparate;

    @Override
    public void initialize(UimaContext context) throws ResourceInitializationException {
        super.initialize(context);
        bDoHandleCombined = pMultiCasHandling == BOTH || pMultiCasHandling == COMBINED;
        bDoHandleSeparate = pMultiCasHandling == BOTH || pMultiCasHandling == SEPARATE;
    }

    @Override
    public void process(JCas jCas) throws AnalysisEngineProcessException {
        try {
            if (!isCasValid(jCas)) return;

            // Initialize study
            int documentLength = JCasUtil.select(jCas, Token.class).size();
            UnitizingAnnotationStudy perCasStudy = new UnitizingAnnotationStudy((int) viewCount, documentLength);
            IndexingMap<String> perCasAnnotatorIndex = new IndexingMap<>();
            UnitizingAnnotationStudy globalStudy = new UnitizingAnnotationStudy((int) viewCount, documentLength);

            // Count all annotations for PARAM_MIN_ANNOTATIONS
            CountMap<String> perViewAnnotationCount = new CountMap<>();

            // Iterate over all views
            for (String fullViewName : validViewNames) {
                JCas viewCas = jCas.getView(fullViewName);
                // Split user id from view name and get annotator index for this id. Discards "_InitialView"
                String viewName = StringUtils.substringAfterLast(fullViewName.trim(), "/");
                perCasAnnotatorIndex.add(viewName);
                globalAnnotatorIndex.add(viewName);

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
                    // Get all not overlapped annotations, filtering fingerprinted if parameter was set
                    ArrayList<? extends Annotation> annotations = getAnnotations(viewCas, fingerprinted, annotationClass);

                    HashMap<? extends Annotation, Collection<Token>> annotationTokenLookup = Maps.newHashMap(indexCovering(viewCas, annotationClass, Token.class));

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
                        categories.add(category);
                        
                        int length = end - begin + 1;
                        perCasStudy.addUnit(
                                begin,
                                length,
                                perCasAnnotatorIndex.get(viewName),
                                category
                        );
                        perViewAnnotationCount.inc(viewName);

                        if (bDoHandleCombined) {
                            globalStudy.addUnit(
                                    begin,
                                    length,
                                    globalAnnotatorIndex.get(viewName),
                                    category
                            );
                        }
                    }
                }
            }

            // Store the collected annotations units and update the document offset for final evaluation
            if (bDoHandleCombined) {
                annotationStudies.add(ImmutablePair.of(documentOffset.get(), perCasStudy.getUnits()));
                documentOffset.getAndAdd(documentLength);
            }

            if (bDoHandleSeparate) {
                handleSeparate(jCas, perCasStudy, perCasAnnotatorIndex);
            }
        } catch (CASException e) {
            getLogger().error("Error in UnitizingIAACollectionProcessingEngine.process().", e);
        }
    }

    @Override
    public void collectionProcessComplete() throws AnalysisEngineProcessException {
        if (globalAnnotatorIndex.size() > 1 && bDoHandleCombined) {
            handleCombined();
        }
        super.collectionProcessComplete();
    }

    private void handleSeparate(JCas jCas, UnitizingAnnotationStudy completeStudy, IndexingMap<String> perCasAnnotatorIndex) {
        if (!pPrintStatistics && !pAnnotateDocument)
            return;

        // Iterate over all previously collected studies
        CountMap<String> categoryCount = new CountMap<>();
        HashMap<String, CountMap<String>> annotatorCategoryCount = new HashMap<>();

        // Initialize a CountMap for each annotator
        for (String annotator : perCasAnnotatorIndex.keySet()) {
            annotatorCategoryCount.put(annotator, new CountMap<>());
        }

        for (IUnitizingAnnotationUnit annotationUnit : completeStudy.getUnits()) {
            int id = annotationUnit.getRaterIdx();
            String category = (String) annotationUnit.getCategory();

            // Update category counts
            categoryCount.inc(category);
            annotatorCategoryCount.get(perCasAnnotatorIndex.getKey(id)).inc(category);
        }

        KrippendorffAlphaUnitizingAgreement agreement = new KrippendorffAlphaUnitizingAgreement(completeStudy);

        if (pPrintStatistics) {
            try {
                String documentId = DocumentMetaData.get(jCas).getDocumentId();
                documentId = documentId != null ? documentId : DocumentMetaData.get(jCas).getDocumentTitle();
                documentId = documentId != null ? documentId : DocumentMetaData.get(jCas).getDocumentUri();

                String fileName = StringUtils.appendIfMissing(StringUtils.removeEnd(documentId, ".xmi"), ".csv");
                CSVPrinter csvPrinter = getCsvPrinter(fileName);
                csvPrinter.printComment(String.format("KrippendorffAlphaUnitizingAgreement - %s",
                        documentId
                ));
                csvPrinter.printComment(String.format("Inter-annotator agreement for %d annotators: %s",
                        perCasAnnotatorIndex.size(), perCasAnnotatorIndex.keySet().toString()
                ));

                // Print the agreement for all categories
                csvPrinter.printRecord("Category", "Count", "Agreement");
                csvPrinter.printRecord("Overall", completeStudy.getUnitCount(), agreement.calculateAgreement());
                printStudyResultsAndStatistics(agreement, categoryCount, annotatorCategoryCount, categories, perCasAnnotatorIndex.keySet(), csvPrinter);
                csvPrinter.flush();
                csvPrinter.close();

                // fileName = StringUtils.appendIfMissing(StringUtils.removeEnd(fileName, ".csv") + "_cm", ".tsv");
                // PrintStream out = new PrintStream(getOutputStream(fileName));
                // new CoincidenceMatrixPrinter().print(out, completeStudy);
                // out.close();
            } catch (IOException e) {
                getLogger().error("Error during statistics printing.", e);
            }
        }

        if (pAnnotateDocument) {
            JCas viewIAA = initializeIaaView(jCas);
            createDocumentAgreementAnnotations(viewIAA, agreement, "KrippendorffAlphaUnitizingAgreement", categories, categoryCount);
        }
    }

    private void handleCombined() {
        if (annotationStudies.isEmpty() || globalAnnotatorIndex.isEmpty())
            return;

        UnitizingAnnotationStudy completeStudy = new UnitizingAnnotationStudy(globalAnnotatorIndex.size(), documentOffset.get());
        CountMap<String> categoryCount = new CountMap<>();
        HashMap<String, CountMap<String>> annotatorCategoryCount = new HashMap<>();

        // Initialize a CountMap for each annotator
        for (String annotator : globalAnnotatorIndex.keySet()) {
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
                annotatorCategoryCount.get(globalAnnotatorIndex.getKey(id)).inc(category);
            }
        }

        if (pPrintStatistics) {
            try {
                CSVPrinter csvPrinter = getCsvPrinter("KrippendorffAlphaUnitizingAgreement.csv");
                csvPrinter.printComment("KrippendorffAlphaUnitizingAgreement, COMBINED");
                csvPrinter.printComment(String.format("Inter-annotator agreement for %d annotators: %s",
                        globalAnnotatorIndex.size(), globalAnnotatorIndex.keySet().toString()
                ));

                // Compute and print the agreement for all categories
                KrippendorffAlphaUnitizingAgreement agreement = new KrippendorffAlphaUnitizingAgreement(completeStudy);

                csvPrinter.printRecord("Category", "Count", "Agreement");
                csvPrinter.printRecord("Overall", completeStudy.getUnitCount(), agreement.calculateAgreement());
                printStudyResultsAndStatistics(agreement, categoryCount, annotatorCategoryCount, categories, globalAnnotatorIndex.keySet(), csvPrinter);
                csvPrinter.flush();
            } catch (IOException e) {
                getLogger().error("Error during statistics printing.", e);
            }
        }
    }

}
