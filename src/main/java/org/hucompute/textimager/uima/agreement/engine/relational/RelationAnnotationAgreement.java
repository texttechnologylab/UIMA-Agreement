package org.hucompute.textimager.uima.agreement.engine.relational;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Streams;
import de.tudarmstadt.ukp.dkpro.core.api.metadata.type.DocumentMetaData;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Token;
import de.tudarmstadt.ukp.dkpro.core.api.semantics.type.WordSense;
import eu.openminted.share.annotations.api.Parameters;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.uima.UimaContext;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.cas.CASException;
import org.apache.uima.fit.descriptor.ConfigurationParameter;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.apache.uima.jcas.cas.TOP;
import org.apache.uima.jcas.tcas.Annotation;
import org.apache.uima.resource.ResourceInitializationException;
import org.dkpro.statistics.agreement.IAnnotationUnit;
import org.dkpro.statistics.agreement.coding.CodingAnnotationStudy;
import org.dkpro.statistics.agreement.coding.KrippendorffAlphaAgreement;
import org.dkpro.statistics.agreement.distance.NominalDistanceFunction;
import org.dkpro.statistics.agreement.unitizing.KrippendorffAlphaUnitizingAgreement;
import org.dkpro.statistics.agreement.unitizing.UnitizingAnnotationStudy;
import org.hucompute.textimager.uima.agreement.engine.AbstractIAAEngine;
import org.texttechnologylab.annotation.SemanticSource;
import org.texttechnologylab.annotation.administration.AnnotationStatus;
import org.texttechnologylab.annotation.semaf.isobase.Entity;
import org.texttechnologylab.annotation.semaf.isobase.Event;
import org.texttechnologylab.annotation.semaf.isobase.Link;
import org.texttechnologylab.annotation.semaf.isobase.Signal;
import org.texttechnologylab.annotation.semaf.semafsr.SrLink;
import org.texttechnologylab.annotation.type.Fingerprint;
import org.texttechnologylab.iaa.AgreementValue;
import org.texttechnologylab.utilities.collections.IndexingMap;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;


@Parameters(
        exclude = {
                AbstractIAAEngine.PARAM_ANNOTATION_CLASSES,
                AbstractIAAEngine.PARAM_MIN_ANNOTATIONS // FIXME: Currently not implemented
        }
)
public class RelationAnnotationAgreement extends AbstractIAAEngine {

    public static final String PARAM_FILTER_PROCESSED = "pFilterProcessed";
    @ConfigurationParameter(
            name = PARAM_FILTER_PROCESSED,
            defaultValue = "true",
            description = "If set to true, only consider predicates that are covered by a AnnotationStatus annotation " +
                    "with status='Processed'"
    )
    protected boolean pFilterProcessed;


    private TreeSet<String> categories = new TreeSet<>();
    private AtomicInteger documentOffset = new AtomicInteger(0);
    private IndexingMap<String> annotatorIndex = new IndexingMap<>();

    @Override
    public void initialize(UimaContext context) throws ResourceInitializationException {
        annotationClasses = ImmutableSet.of(Entity.class, Event.class, Signal.class);
        super.initialize(context);
        switch (pMultiCasHandling) {
            case COMBINED:
            case BOTH:
                logger.warn(String.format("RelationAnnotationAgreement.PARAM_MULTI_CAS_HANDLING=%s is not implemented, " +
                        "defaulting to SEPARATE.", pMultiCasHandling));
                break;
        }
    }

    @Override
    public void process(JCas jCas) throws AnalysisEngineProcessException {
        try {
            if (!isCasValid(jCas)) return;

            // Initialize study
            int documentLength = JCasUtil.select(jCas, Token.class).size();
            CodingAnnotationStudy predicateIdentificationStudy = new CodingAnnotationStudy((int) viewCount);

            CodingAnnotationStudy predicateDisambiguationStudyTTLab = new CodingAnnotationStudy((int) viewCount);
            CodingAnnotationStudy predicateDisambiguationStudyPropBank = new CodingAnnotationStudy((int) viewCount);

            UnitizingAnnotationStudy argumentIdentificationStudyPropBank = new UnitizingAnnotationStudy((int) viewCount, documentLength);
            UnitizingAnnotationStudy argumentClassificationStudyPropBank = new UnitizingAnnotationStudy((int) viewCount, documentLength);
            UnitizingAnnotationStudy argumentClassificationMatchingSpansStudyPropBank = new UnitizingAnnotationStudy((int) viewCount, documentLength);

            UnitizingAnnotationStudy argumentIdentificationStudyTTLab = new UnitizingAnnotationStudy((int) viewCount, documentLength);
            UnitizingAnnotationStudy argumentClassificationStudyTTLab = new UnitizingAnnotationStudy((int) viewCount, documentLength);
            UnitizingAnnotationStudy argumentClassificationMatchingSpansStudyTTLab = new UnitizingAnnotationStudy((int) viewCount, documentLength);

            HashMap<Integer, AnnotationContainer> perViewSRLContainers = new HashMap<>();

            // Iterate over all views
            for (String fullViewName : validViewNames) {
                JCas viewCas = jCas.getView(fullViewName);
                // Split user id from view name and get annotator index for this id. Discards "_InitialView"
                String viewName = StringUtils.substringAfterLast(fullViewName.trim(), "/");
                annotatorIndex.add(viewName);

                Integer raterIdx = annotatorIndex.get(viewName);
                perViewSRLContainers.put(raterIdx, new AnnotationContainer(viewCas, viewName, raterIdx));
            }

            // Create a set of all multi-tokens, that are covering another token
            IndexingMap<Token> tokenIndexingMap = getIndexingMap(jCas, Token.class);

            Map<Sentence, Collection<Token>> sentenceTokenIndex = JCasUtil.indexCovered(jCas, Sentence.class, Token.class);

            IndexingMap<Sentence> sentenceIndexingMap = getIndexingMap(jCas, Sentence.class);
            for (int i = 0; i < sentenceIndexingMap.size(); i++) {
                Collection<Token> tokens = sentenceTokenIndex.get(sentenceIndexingMap.getKey(i));
                List<Integer> tokenIndices = tokens.stream().map(tokenIndexingMap::get).collect(Collectors.toList());
                for (Integer tokenIndex : tokenIndices) {
                    /* Predicate Identification */
                    // Get the predicate annotations for the current token (by index), if present
                    Entity[] predicateIdentificationAnnotations = runPredicateIdentification(predicateIdentificationStudy, perViewSRLContainers, tokenIndex);

                    /* Predicate Disambiguation */
                    // If all annotators agreed that the current token is a predicate,
                    // continue with the evaluation of the other tasks
                    if (Arrays.stream(predicateIdentificationAnnotations).map(e -> e == null ? "O" : "P").allMatch(Predicate.isEqual("P"))) {
                        // Get all SemanticSource annotations that cover the current predicate
                        List<Collection<SemanticSource>> semanticSourcesCoveringCurrentPredicate = getSemanticSourcesCoveringCurrentPredicate(perViewSRLContainers, tokenIndex);

                        // If there is a view without any SemanticSources, skip this sample entirely
                        if (semanticSourcesCoveringCurrentPredicate.stream().anyMatch(Objects::isNull)) {
                            logger.warn(String.format(
                                    "At least one view is missing a SemanticSource annotation covering the predicate '%s' at tokenIndex %d",
                                    tokenIndexingMap.getKey(tokenIndex).getCoveredText(), tokenIndex
                            ));
                            continue;
                        }

                        // Create a mapping of the SemanticSource variant (TTLab or PropBank) to its respective annotation
                        // TODO: This currently discards multiple SemanticSource annotations of the same predicate, retaining only one.
                        // TODO: Check if multiple SemanticSource annotations are supposed to happen!
                        List<Map<String, SemanticSource>> mappedSemanticSources = getMappedSemanticSources(semanticSourcesCoveringCurrentPredicate);

                        /// Predicate Disambiguation -- TTLab Verbset
                        // Get the SemanticSource annotations for the current predicate that use the TTLab synset
                        String[] ttlabDisambiguationAnnotationLabels = runPredicateDisambiguation(
                                predicateDisambiguationStudyTTLab,
                                mappedSemanticSources,
                                "ttlabsynset"
                        );

                        /// Predicate Disambiguation -- PropBank Verbset
                        String[] propbankDisambiguationAnnotationLabels = runPredicateDisambiguation(
                                predicateDisambiguationStudyPropBank,
                                mappedSemanticSources,
                                "propbank"
                        );

                        /* Argument Identification & Classification */
                        // Check if all sense labels for the current predicate match
                        // TODO: Check if this is necessary.

                        /// Argument Identification & Classification -- TTLab Verbset
                        if (Arrays.stream(ttlabDisambiguationAnnotationLabels).allMatch(Predicate.isEqual(ttlabDisambiguationAnnotationLabels[0]))) {
                            runArgumentIdentificationClassification(
                                    argumentIdentificationStudyTTLab,
                                    argumentClassificationStudyTTLab,
                                    argumentClassificationMatchingSpansStudyTTLab,
                                    perViewSRLContainers,
                                    predicateIdentificationAnnotations
                            );
                        }

                        /// Argument Identification & Classification -- PropBank Verbset
                        if (Arrays.stream(propbankDisambiguationAnnotationLabels).allMatch(Predicate.isEqual(propbankDisambiguationAnnotationLabels[0]))) {
                            runArgumentIdentificationClassification(
                                    argumentIdentificationStudyPropBank,
                                    argumentClassificationStudyPropBank,
                                    argumentClassificationMatchingSpansStudyPropBank,
                                    perViewSRLContainers,
                                    predicateIdentificationAnnotations
                            );
                        }
                    }
                }
            }

            documentOffset.getAndAdd(documentLength);

            switch (pMultiCasHandling) {
                case SEPARATE:
                case COMBINED:
                case BOTH:
                default:
                    handleSeparate(
                            jCas,
                            predicateIdentificationStudy,
                            predicateDisambiguationStudyTTLab,
                            predicateDisambiguationStudyPropBank,
                            argumentIdentificationStudyPropBank,
                            argumentClassificationStudyPropBank,
                            argumentClassificationMatchingSpansStudyPropBank,
                            argumentIdentificationStudyTTLab,
                            argumentClassificationStudyTTLab,
                            argumentClassificationMatchingSpansStudyTTLab
                    );
                    break;
            }
        } catch (CASException e) {
            e.printStackTrace();
        }
    }

    @Nonnull
    protected JCas initializeIaaView(JCas jCas) {
        JCas viewIAA = JCasUtil.getView(jCas, "IAA", true);
        if (viewIAA.getDocumentText() == null)
            viewIAA.setDocumentText(jCas.getDocumentText());
        viewIAA.removeAllIncludingSubtypes(AgreementValue.type);
        return viewIAA;
    }

    private void handleSeparate(
            JCas jCas,
            CodingAnnotationStudy predicateIdentificationStudy,
            CodingAnnotationStudy predicateDisambiguationStudyTTLab,
            CodingAnnotationStudy predicateDisambiguationStudyPropBank,
            UnitizingAnnotationStudy argumentIdentificationStudyPropBank,
            UnitizingAnnotationStudy argumentClassificationStudyPropBank,
            UnitizingAnnotationStudy argumentClassificationMatchingSpansStudyPropBank,
            UnitizingAnnotationStudy argumentIdentificationStudyTTLab,
            UnitizingAnnotationStudy argumentClassificationStudyTTLab,
            UnitizingAnnotationStudy argumentClassificationMatchingSpansStudyTTLab
    ) {
        if (pAnnotateDocument || pPrintStatistics) {
            long predicateIdentificationPositiveSamples = Streams.stream(predicateIdentificationStudy.getItems()).filter(i -> Streams.stream(i.getUnits()).map(IAnnotationUnit::getCategory).anyMatch(Predicate.isEqual("P"))).count();
            long predicateIdentificationDoublePositiveSamples = Streams.stream(predicateIdentificationStudy.getItems()).filter(i -> Streams.stream(i.getUnits()).map(IAnnotationUnit::getCategory).allMatch(Predicate.isEqual("P"))).count();
            double predicateIdentificationAgreementValue = calculateCodingAgreement(predicateIdentificationStudy);

            double predicateDisambiguationStudyTTLabAgreementValue = calculateCodingAgreement(predicateDisambiguationStudyTTLab);
            double predicateDisambiguationStudyPropBankAgreementValue = calculateCodingAgreement(predicateDisambiguationStudyPropBank);

            double argumentIdentificationStudyTTLabAgreementValue = calculateUnitzingAgreement(argumentIdentificationStudyTTLab);
            double argumentIdentificationStudyPropBankAgreementValue = calculateUnitzingAgreement(argumentIdentificationStudyPropBank);

            double argumentClassificationStudyTTLabAgreementValue = calculateUnitzingAgreement(argumentClassificationStudyTTLab);
            double argumentClassificationStudyPropBankAgreementValue = calculateUnitzingAgreement(argumentClassificationStudyPropBank);

            double argumentClassificationMatchingSpansStudyTTLabAgreementValue = calculateUnitzingAgreement(argumentClassificationMatchingSpansStudyTTLab);
            double argumentClassificationMatchingSpansStudyPropBankAgreementValue = calculateUnitzingAgreement(argumentClassificationMatchingSpansStudyPropBank);

            if (pAnnotateDocument) {
                JCas viewIAA = initializeIaaView(jCas);
                AgreementValue agreementValue;

                agreementValue = new AgreementValue(viewIAA);
                agreementValue.setAgreementLabel("Predicate Identification");
                agreementValue.setAgreementValue(predicateIdentificationAgreementValue);
                agreementValue.setAgreementUnits((int) predicateIdentificationPositiveSamples);
                viewIAA.addFsToIndexes(agreementValue);

                agreementValue = new AgreementValue(viewIAA);
                agreementValue.setAgreementLabel("Predicate Disambiguation - TTLab");
                agreementValue.setAgreementValue(predicateDisambiguationStudyTTLabAgreementValue);
                agreementValue.setAgreementUnits(predicateDisambiguationStudyTTLab.getItemCount());
                viewIAA.addFsToIndexes(agreementValue);

                agreementValue = new AgreementValue(viewIAA);
                agreementValue.setAgreementLabel("Predicate Disambiguation - PropBank");
                agreementValue.setAgreementValue(predicateDisambiguationStudyPropBankAgreementValue);
                agreementValue.setAgreementUnits(predicateDisambiguationStudyPropBank.getItemCount());
                viewIAA.addFsToIndexes(agreementValue);

                agreementValue = new AgreementValue(viewIAA);
                agreementValue.setAgreementLabel("Argument Identification - TTLab");
                agreementValue.setAgreementValue(argumentIdentificationStudyTTLabAgreementValue);
                agreementValue.setAgreementUnits(argumentIdentificationStudyTTLab.getUnitCount());
                viewIAA.addFsToIndexes(agreementValue);

                agreementValue = new AgreementValue(viewIAA);
                agreementValue.setAgreementLabel("Argument Identification - PropBank");
                agreementValue.setAgreementValue(argumentIdentificationStudyPropBankAgreementValue);
                agreementValue.setAgreementUnits(argumentIdentificationStudyPropBank.getUnitCount());
                viewIAA.addFsToIndexes(agreementValue);

                agreementValue = new AgreementValue(viewIAA);
                agreementValue.setAgreementLabel("Argument Classification (All Spans) - TTLab");
                agreementValue.setAgreementValue(argumentClassificationStudyTTLabAgreementValue);
                agreementValue.setAgreementUnits(argumentClassificationStudyTTLab.getUnitCount());
                viewIAA.addFsToIndexes(agreementValue);

                agreementValue = new AgreementValue(viewIAA);
                agreementValue.setAgreementLabel("Argument Classification (All Spans) - PropBank");
                agreementValue.setAgreementValue(argumentClassificationStudyPropBankAgreementValue);
                agreementValue.setAgreementUnits(argumentClassificationStudyPropBank.getUnitCount());
                viewIAA.addFsToIndexes(agreementValue);

                agreementValue = new AgreementValue(viewIAA);
                agreementValue.setAgreementLabel("Argument Classification (Matching Spans) - TTLab");
                agreementValue.setAgreementValue(argumentClassificationMatchingSpansStudyTTLabAgreementValue);
                agreementValue.setAgreementUnits(argumentClassificationMatchingSpansStudyTTLab.getUnitCount());
                viewIAA.addFsToIndexes(agreementValue);

                agreementValue = new AgreementValue(viewIAA);
                agreementValue.setAgreementLabel("Argument Classification (Matching Spans) - PropBank");
                agreementValue.setAgreementValue(argumentClassificationMatchingSpansStudyPropBankAgreementValue);
                agreementValue.setAgreementUnits(argumentClassificationMatchingSpansStudyPropBank.getUnitCount());
                viewIAA.addFsToIndexes(agreementValue);
            }

            if (pPrintStatistics) {
                System.out.printf("%s,%s filtering 'Processed' samples\n", StringUtils.appendIfMissing(DocumentMetaData.get(jCas).getDocumentId(), ".xmi"), pFilterProcessed ? "" : " not");
                System.out.println("Predicate Identification                 && \\\\");
                System.out.printf("$\\quad$ All                              & %01.6f & %d items, %d double positive \\\\\n", predicateIdentificationAgreementValue, predicateIdentificationPositiveSamples, predicateIdentificationDoublePositiveSamples);
                System.out.println("Predicate Disambiguation                 && \\\\");
                System.out.printf("$\\quad$ TTLab                            & %01.6f & %d items \\\\\n", predicateDisambiguationStudyTTLabAgreementValue, predicateDisambiguationStudyTTLab.getItemCount());
                System.out.printf("$\\quad$ PropBank                         & %01.6f & %d items \\\\\n", predicateDisambiguationStudyPropBankAgreementValue, predicateDisambiguationStudyPropBank.getItemCount());
                System.out.println("Argument Identification                  && \\\\");
                System.out.printf("$\\quad$ TTLab                            & %01.6f & %d units \\\\\n", argumentIdentificationStudyTTLabAgreementValue, argumentIdentificationStudyTTLab.getUnitCount());
                System.out.printf("$\\quad$ PropBank                         & %01.6f & %d units \\\\\n", argumentIdentificationStudyPropBankAgreementValue, argumentIdentificationStudyPropBank.getUnitCount());
                System.out.println("Argument Classification (All)            && \\\\");
                System.out.printf("$\\quad$ TTLab                            & %01.6f & %d units \\\\\n", argumentClassificationStudyTTLabAgreementValue, argumentClassificationStudyTTLab.getUnitCount());
                System.out.printf("$\\quad$ PropBank                         & %01.6f & %d units \\\\\n", argumentClassificationStudyPropBankAgreementValue, argumentClassificationStudyPropBank.getUnitCount());
                System.out.println("Argument Classification (Matching Spans) && \\\\");
                System.out.printf("$\\quad$ TTLab                            & %01.6f & %d units \\\\\n", argumentClassificationMatchingSpansStudyTTLabAgreementValue, argumentClassificationMatchingSpansStudyTTLab.getUnitCount());
                System.out.printf("$\\quad$ PropBank                         & %01.6f & %d units \\\\\n", argumentClassificationMatchingSpansStudyPropBankAgreementValue, argumentClassificationMatchingSpansStudyPropBank.getUnitCount());
                System.out.flush();
            }
        }
    }

    private double calculateCodingAgreement(CodingAnnotationStudy predicateDisambiguationStudyTTLab) {
        return new KrippendorffAlphaAgreement(predicateDisambiguationStudyTTLab, new NominalDistanceFunction()).calculateAgreement();
    }

    private double calculateUnitzingAgreement(UnitizingAnnotationStudy argumentIdentificationStudyTTLab) {
        return new KrippendorffAlphaUnitizingAgreement(argumentIdentificationStudyTTLab).calculateAgreement();
    }

    private Entity[] runPredicateIdentification(CodingAnnotationStudy predicateIdentificationStudy, HashMap<Integer, AnnotationContainer> perViewSRLContainers, Integer tokenIndex) {
        Entity[] predicateIdentificationAnnotations = perViewSRLContainers.values()
                .stream()
                .map(c -> c.getPredicateCoveringTokenByIndex(tokenIndex))
                .toArray(Entity[]::new);

        // Map the predicate annotations to binary "predicate-present/out" labels (P/O)
        Object[] predicateIdentificationAnnotationLabels = Arrays.stream(predicateIdentificationAnnotations)
                .map(e -> e == null ? "O" : "P")
                .toArray();
        predicateIdentificationStudy.addItemAsArray(
                predicateIdentificationAnnotationLabels
        );
        return predicateIdentificationAnnotations;
    }

    private List<Collection<SemanticSource>> getSemanticSourcesCoveringCurrentPredicate(HashMap<Integer, AnnotationContainer> perViewSRLContainers, Integer tokenIndex) {
        return perViewSRLContainers.values()
                .stream()
                .map(c -> c.entitySemanticSourceLookup.get(c.getPredicateCoveringTokenByIndex(tokenIndex)))
                .collect(Collectors.toList());
    }

    private List<Map<String, SemanticSource>> getMappedSemanticSources(List<Collection<SemanticSource>> semanticSourcesCoveringCurrentPredicate) {
        return semanticSourcesCoveringCurrentPredicate.stream()
                .map(c -> c.stream().filter(semanticSource -> semanticSource.getSource() != null).collect(Collectors.toList()))
                .map(c -> c.stream().collect(Collectors.toMap(
                        SemanticSource::getSource,
                        Function.identity(),
                        (a, b) -> a))
                ).collect(Collectors.toList());
    }

    private String[] runPredicateDisambiguation(
            final CodingAnnotationStudy perCasPredicateDisambiguationStudy,
            final List<Map<String, SemanticSource>> mappedSemanticSources,
            final String versetIdentifier
    ) {
        SemanticSource[] propbankDisambiguationAnnotations = mappedSemanticSources.stream()
                .map(map -> map.getOrDefault(versetIdentifier, null))
                .toArray(SemanticSource[]::new);

        // Store the sense labels (using the "value" field of the SemanticSource annotation) in an array
        // for the Argument Identification & Classification tasks
        String[] propbankDisambiguationAnnotationLabels = Arrays.stream(propbankDisambiguationAnnotations)
                .map(Optional::ofNullable)
                .map(o -> o.map(WordSense::getValue).orElse(null))
                .toArray(String[]::new);

        // Add an item to the corresponding study if at least one annotation is not null
        if (Arrays.stream(propbankDisambiguationAnnotationLabels).anyMatch(Objects::nonNull)) {
            perCasPredicateDisambiguationStudy.addItemAsArray(
                    propbankDisambiguationAnnotationLabels
            );
        }
        return propbankDisambiguationAnnotationLabels;
    }

    private void runArgumentIdentificationClassification(
            final UnitizingAnnotationStudy perCasArgumentIdentificationStudy,
            final UnitizingAnnotationStudy perCasArgumentClassificationStudy,
            final UnitizingAnnotationStudy perCasArgumentClassificationMatchingSpansStudy,
            final HashMap<Integer, AnnotationContainer> perViewSRLContainers,
            final Entity[] predicateIdentificationAnnotations
    ) {
        HashMap<ImmutablePair<Integer, Integer>, HashMap<Integer, String>> argumentDisambiguationSpans = new HashMap<>();
        for (int raterIndex = 0; raterIndex < annotatorIndex.size(); raterIndex++) {
            Entity predicate = predicateIdentificationAnnotations[raterIndex];
            AnnotationContainer annotationContainer = perViewSRLContainers.get(raterIndex);
            HashMap<Entity, String> argumentsOfPredicate = annotationContainer.predicateArgumentLookup.get(predicate);
            for (Map.Entry<Entity, String> argumentAndLabel : argumentsOfPredicate.entrySet()) {
                Entity argument = argumentAndLabel.getKey();

                /// Argument Identification
                createAnnotation(
                        perCasArgumentIdentificationStudy,
                        raterIndex,
                        annotationContainer.argumentTokenLookup.get(argument),
                        annotationContainer.tokenIndexingMap,
                        "ARG"
                );

                /// Argument Disambiguation
                String label = argumentAndLabel.getValue();
                ImmutablePair<Integer, Integer> beginLengthPair = createAnnotation(
                        perCasArgumentClassificationStudy,
                        raterIndex,
                        annotationContainer.argumentTokenLookup.get(argument),
                        annotationContainer.tokenIndexingMap,
                        label
                );

                if (beginLengthPair != null) {
                    HashMap<Integer, String> annotatorLabelMap = argumentDisambiguationSpans.getOrDefault(beginLengthPair, new HashMap<>());
                    annotatorLabelMap.put(raterIndex, label);
                    argumentDisambiguationSpans.put(beginLengthPair, annotatorLabelMap);
                }
            }
        }
        argumentDisambiguationSpans.forEach((span, annotatorMap) -> {
            if (annotatorMap.size() == annotatorIndex.size()) {
                // All annotators agree about this span
                annotatorMap.forEach((raterIndex, label) -> {
                    perCasArgumentClassificationMatchingSpansStudy.addUnit(
                            span.getLeft(),
                            span.getRight(),
                            raterIndex,
                            label
                    );
                });
            }
        });
    }

    public static <T extends Annotation> IndexingMap<T> getIndexingMap(JCas someCas, Class<T> type) {
        HashSet<T> coveredAnnotations = JCasUtil.indexCovering(someCas, type, type).entrySet().stream()
                .filter(annotationsCoveredByThisOne -> annotationsCoveredByThisOne.getValue().size() > 1)
                .map(Map.Entry::getKey)
                .collect(Collectors.toCollection(HashSet::new));

        // Create an index for the token, that are not part of sub-token
        IndexingMap<T> indexingMap = new IndexingMap<>();
        JCasUtil.select(someCas, type).stream()
                .filter(((Predicate<T>) coveredAnnotations::contains).negate())
                .forEachOrdered(indexingMap::add);

        return indexingMap;
    }

    /**
     * Create an annotation in the given unitizing study for the given annotator. This method will establish the span of
     * the annotation by finding min/max of begin/end of the tokens in the 'containedTokens' collection.
     *
     * @param unitizingAnnotationStudy The study to add the unit to.
     * @param raterIdx                 The rater index of the annotation.
     * @param containedTokens          The tokens contained within / covered by the annotation in question.
     * @param tokenIndexingMap         An {@link IndexingMap IndexingMap<Token>} that will be used to determine min/max
     *                                 of the start and end indices of the contained tokens.
     * @param category                 The category label of the unit.
     * @return
     */
    private ImmutablePair<Integer, Integer> createAnnotation(
            UnitizingAnnotationStudy unitizingAnnotationStudy,
            Integer raterIdx,
            Collection<Token> containedTokens,
            IndexingMap<Token> tokenIndexingMap,
            String category
    ) {

        // initialize indexes
        int begin = Integer.MAX_VALUE;
        int end = Integer.MIN_VALUE;


        for (Token token : containedTokens) {
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
            return null;
        }

        int length = end - begin + 1;
        unitizingAnnotationStudy.addUnit(
                begin,
                length,
                raterIdx,
                category
        );
        categories.add(category);
        return new ImmutablePair<>(begin, length);
    }

    public class AnnotationContainer {
        private final JCas viewCas;
        private final String viewName;
        private final Integer raterIdx;
        private final IndexingMap<Token> tokenIndexingMap;
        private final IndexingMap<Sentence> sentenceIndexingMap;
        private final ArrayList<? extends Link> links;
        private final ArrayList<? extends Entity> predicates;
        private final ArrayList<? extends Entity> arguments;
        private final Map<Sentence, Collection<Entity>> sentencesCoveredPredicates;
        private final Map<Entity, Collection<Token>> entityTokenLookup;
        private final Map<Entity, Collection<Token>> predicateTokenLookup;
        private final Map<Entity, Collection<Token>> argumentTokenLookup;
        private final Map<Token, Entity> tokenPredicateLookup;
        private final HashMap<Entity, HashMap<Entity, String>> predicateArgumentLookup;
        private final Map<Entity, Collection<SemanticSource>> entitySemanticSourceLookup;

        public AnnotationContainer(JCas viewCas, String viewName, Integer raterIdx) {
            this.viewCas = viewCas;
            this.viewName = viewName;
            this.raterIdx = raterIdx;

            // Create an index for the token, that are not part of sub-token
            this.tokenIndexingMap = getIndexingMap(viewCas, Token.class);
            this.sentenceIndexingMap = getIndexingMap(viewCas, Sentence.class);

            // Get all fingerprinted annotations
            HashSet<TOP> fingerprinted = JCasUtil.select(viewCas, Fingerprint.class).stream()
                    .map(Fingerprint::getReference)
                    .collect(Collectors.toCollection(HashSet::new));

            // Each relational annotation consists of a Link between a ground and a figure/trigger of (base-)class Entity
            this.links = getLinks(viewCas, fingerprinted);
            this.predicates = this.links.stream().map(Link::getFigure).distinct().collect(Collectors.toCollection(ArrayList::new));
            if (pFilterProcessed) {
                List<AnnotationStatus> annotationStatusProcessed = JCasUtil.select(viewCas, AnnotationStatus.class).stream()
                        .filter(status -> "Processed".equals(status.getStatus()))
                        .collect(Collectors.toList());

                List<Entity> annotationStatusProcessedEntities = Maps.filterKeys(
                        JCasUtil.indexCovered(viewCas, AnnotationStatus.class, Entity.class),
                        annotationStatusProcessed::contains
                ).values().stream().flatMap(Collection::stream).collect(Collectors.toList());
                this.predicates.retainAll(annotationStatusProcessedEntities);
            }
            this.arguments = this.links.stream().map(Link::getGround).collect(Collectors.toCollection(ArrayList::new));

            this.entitySemanticSourceLookup = Maps.filterValues(
                    JCasUtil.indexCovering(viewCas, Entity.class, SemanticSource.class),
                    list -> {
                        list.removeAll(list.stream().filter(ws -> (ws.getBegin() == 0 && ws.getEnd() == 0)).collect(Collectors.toList()));
                        list.removeAll(list.stream().filter(ws -> (ws.getValue() == null || ws.getValue().equals("null"))).collect(Collectors.toList()));
                        return list.size() > 0;
                    }
            );

            predicateArgumentLookup = new HashMap<>();
            for (Link link : this.links) {
                HashMap<Entity, String> arguments = predicateArgumentLookup.getOrDefault(link.getFigure(), new HashMap<>());
                arguments.put(link.getGround(), link.getRel_type());
                predicateArgumentLookup.put(link.getFigure(), arguments);
            }

            // Create a mapping of sentences to the entites they cover
            this.sentencesCoveredPredicates = Maps.filterValues(
                    JCasUtil.indexCovered(viewCas, Sentence.class, Entity.class),
                    list -> {
                        // Filter the entities for predicates
                        list.retainAll(this.predicates);
                        // Keep only sentences covering at least one predicate
                        return list.size() > 0;
                    }
            );

            // Create a lookup for the tokens that are covered by an entity
            this.entityTokenLookup = JCasUtil.indexCovered(viewCas, Entity.class, Token.class);

            // Create a lookup for the tokens that are covered by an entity
            this.predicateTokenLookup = Maps.filterKeys(
                    this.entityTokenLookup,
                    this.predicates::contains
            );

            // Create a lookup for the tokens that are covered by an argument
            this.argumentTokenLookup = Maps.filterKeys(
                    this.entityTokenLookup,
                    this.arguments::contains
            );

            // Create a lookup for the tokens that are covered by an entity
            this.tokenPredicateLookup = reverseMapFlat(this.predicateTokenLookup);
        }

        public Entity getPredicateCoveringTokenByIndex(Integer index) {
            Entity predicate = null;
            Token token = this.tokenIndexingMap.getKey(index);
            if (this.tokenPredicateLookup.containsKey(token)) {
                predicate = this.tokenPredicateLookup.get(token);
            }
            return predicate;
        }

        @Nonnull
        protected ArrayList<? extends Link> getLinks(JCas viewCas, HashSet<TOP> fingerprinted) {
            ArrayList<? extends Link> annotations;
            if (pFilterFingerprinted)
                annotations = JCasUtil.select(viewCas, SrLink.class).stream()
                        .filter((Predicate<TOP>) fingerprinted::contains)
                        .collect(Collectors.toCollection(ArrayList::new));
            else
                annotations = new ArrayList<>(JCasUtil.select(viewCas, Link.class));

            return annotations;
        }
    }

    /**
     * Generic method to reverse a {@link Map Map{K, Collection{V}}}. The returned map will contain a <b>flat</b>
     * mapping of the values in the inner collections to their corresponding keys.
     * <p>
     * This method will <b>not check for key conflicts</b> with duplicate values in the inner collections, neither in
     * the same collection, nor in collections across differnt keys.
     *
     * @param inputMap The map to be reveresed.
     * @param <K>      The key type.
     * @param <V>      The value type.
     * @return A new {@link HashMap} containing the reversed, flattened mapping.
     */
    public static <K, V> HashMap<V, K> reverseMapFlat(Map<K, Collection<V>> inputMap) {
        HashMap<V, K> reversedMap = new HashMap<>();
        inputMap.forEach((key, values) -> {
            for (V value : values) {
                reversedMap.put(value, key);
            }
        });
        return reversedMap;
    }

    @Deprecated
    public static <K, V> HashMap<V, Collection<K>> reverseMap(Map<K, Collection<V>> inputMap) {
        HashMap<V, Collection<K>> reversedMap = new HashMap<>();
        inputMap.forEach((entity, tokens) -> {
            for (V token : tokens) {
                if (!reversedMap.containsKey(token)) {
                    try {
                        Collection<K> newCollection = (Collection<K>) inputMap.getClass().newInstance();
                        newCollection.add(entity);
                        reversedMap.put(token, newCollection);
                    } catch (InstantiationException e) {
                        e.printStackTrace();
                    } catch (IllegalAccessException e) {
                        e.printStackTrace();
                    }
                } else {
                    reversedMap.get(token).add(entity);
                }
            }
        });
        return reversedMap;
    }

}
