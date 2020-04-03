package org.texttechnologylab.agreement.engine;

import com.google.common.io.Files;
import org.apache.uima.collection.CollectionReader;
import org.apache.uima.fit.factory.AggregateBuilder;
import org.apache.uima.fit.factory.AnalysisEngineFactory;
import org.apache.uima.fit.factory.CollectionReaderFactory;
import org.apache.uima.fit.pipeline.SimplePipeline;
import org.dkpro.core.io.xmi.XmiReader;
import org.junit.Test;
import org.texttechnologylab.annotation.AbstractNamedEntity;
import org.texttechnologylab.annotation.NamedEntity;
import org.texttechnologylab.utilities.uima.reader.TextAnnotatorRepositoryCollectionReader;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;

import static org.junit.Assert.fail;

/**
 * Created on 28.01.2019.
 */
public class InterAnnotatorAgreementEngineTest {
    @Test
    public void testAnnotatorAgreement() {
        try {
            final boolean download = false;

            String xmiPath = "src/test/resources/out/xmi/";
            CollectionReader collection;
            if (download) {
                String txtPath = "src/test/resources/out/txt/";
                Paths.get(xmiPath).toFile().mkdirs();
                Paths.get(txtPath).toFile().mkdirs();
                String sessionId = Files.asCharSource(new File("src/test/resources/session.id"), StandardCharsets.UTF_8).read().trim();
                collection = CollectionReaderFactory.createReader(
                        TextAnnotatorRepositoryCollectionReader.class,
                        TextAnnotatorRepositoryCollectionReader.PARAM_SOURCE_LOCATION, xmiPath,
                        TextAnnotatorRepositoryCollectionReader.PARAM_TARGET_LOCATION, txtPath,
                        TextAnnotatorRepositoryCollectionReader.PARAM_SESSION_ID, sessionId,
                        TextAnnotatorRepositoryCollectionReader.PARAM_FORCE_RESERIALIZE, true
//						, XmiReader.PARAM_LOG_FREQ, -1
                );
            } else {
                collection = CollectionReaderFactory.createReader(
                        XmiReader.class,
                        XmiReader.PARAM_PATTERNS, "[+]*.xmi",
                        XmiReader.PARAM_SOURCE_LOCATION, xmiPath,
                        XmiReader.PARAM_LENIENT, true
//						, XmiReader.PARAM_LOG_FREQ, -1
                );
            }

            AggregateBuilder ab = new AggregateBuilder();

            // Test parameters
            String[] annotatorWhitelist = {"305236", "305235"};
            String[] annotatorBlacklist = {"0", "302904"};
            boolean filterFingerprinted = true;
            String[] annotationClasses = {NamedEntity.class.getName(), AbstractNamedEntity.class.getName()};

            ab.add(AnalysisEngineFactory.createEngineDescription(
                    CsvPrinterEngine.class,
                    CsvPrinterEngine.PARAM_TARGET_LOCATION, "src/test/resources/out/annotations.csv",
                    CsvPrinterEngine.PARAM_MIN_VIEWS, 2,
//					CsvPrinterEngine.PARAM_ANNOTATOR_LIST, annotatorWhitelist,
//					CsvPrinterEngine.PARAM_ANNOTATOR_RELATION, UnitizingIAACollectionProcessingEngine.WHITELIST,
                    CsvPrinterEngine.PARAM_ANNOTATOR_LIST, annotatorBlacklist,
                    CsvPrinterEngine.PARAM_ANNOTATOR_RELATION, UnitizingIAACollectionProcessingEngine.BLACKLIST,
                    CsvPrinterEngine.PARAM_FILTER_FINGERPRINTED, filterFingerprinted
            ));

            String[] unitizingIncludeFlags = new String[]{TTLabUnitizingIAACollectionProcessingEngine.METAPHOR, TTLabUnitizingIAACollectionProcessingEngine.METONYM, TTLabUnitizingIAACollectionProcessingEngine.SPECIFIC};
            ab.add(AnalysisEngineFactory.createEngineDescription(
                    TTLabUnitizingIAACollectionProcessingEngine.class,
                    TTLabUnitizingIAACollectionProcessingEngine.PARAM_ANNOTATION_CLASSES, annotationClasses,
                    TTLabUnitizingIAACollectionProcessingEngine.PARAM_INCLUDE_FLAGS, unitizingIncludeFlags,
                    TTLabUnitizingIAACollectionProcessingEngine.PARAM_MIN_VIEWS, 2,
                    TTLabUnitizingIAACollectionProcessingEngine.PARAM_ANNOTATOR_LIST, annotatorWhitelist,
                    TTLabUnitizingIAACollectionProcessingEngine.PARAM_ANNOTATOR_RELATION, UnitizingIAACollectionProcessingEngine.WHITELIST,
//					TTLabUnitizingIAACollectionProcessingEngine.PARAM_ANNOTATOR_LIST, annotatorBlacklist,
//					TTLabUnitizingIAACollectionProcessingEngine.PARAM_ANNOTATOR_RELATION, UnitizingIAACollectionProcessingEngine.BLACKLIST,
                    TTLabUnitizingIAACollectionProcessingEngine.PARAM_FILTER_FINGERPRINTED, filterFingerprinted,
                    TTLabUnitizingIAACollectionProcessingEngine.PARAM_MULTI_CAS_HANDLING, TTLabUnitizingIAACollectionProcessingEngine.BOTH,
                    TTLabUnitizingIAACollectionProcessingEngine.PARAM_MIN_ANNOTATIONS, 10,
                    TTLabUnitizingIAACollectionProcessingEngine.PARAM_TARGET_LOCATION, "src/test/resources/out/unitizing"
            ));

            String[] codingIncludeFlags = new String[]{TTLabCodingIAACollectionProcessingEngine.METAPHOR, TTLabCodingIAACollectionProcessingEngine.METONYM, TTLabCodingIAACollectionProcessingEngine.SPECIFIC};
            ab.add(AnalysisEngineFactory.createEngineDescription(
                    TTLabCodingIAACollectionProcessingEngine.class,
                    TTLabCodingIAACollectionProcessingEngine.PARAM_ANNOTATION_CLASSES, annotationClasses,
                    TTLabCodingIAACollectionProcessingEngine.PARAM_INCLUDE_FLAGS, codingIncludeFlags,
                    TTLabCodingIAACollectionProcessingEngine.PARAM_MIN_VIEWS, 2,
                    TTLabCodingIAACollectionProcessingEngine.PARAM_ANNOTATOR_LIST, annotatorWhitelist,
                    TTLabCodingIAACollectionProcessingEngine.PARAM_ANNOTATOR_RELATION, TTLabCodingIAACollectionProcessingEngine.WHITELIST,
//					TTLabCodingIAACollectionProcessingEngine.PARAM_ANNOTATOR_LIST, annotatorBlacklist,
//					TTLabCodingIAACollectionProcessingEngine.PARAM_ANNOTATOR_RELATION, UnitizingIAACollectionProcessingEngine.BLACKLIST,
                    TTLabCodingIAACollectionProcessingEngine.PARAM_FILTER_FINGERPRINTED, filterFingerprinted,
                    TTLabCodingIAACollectionProcessingEngine.PARAM_AGREEMENT_MEASURE, TTLabCodingIAACollectionProcessingEngine.KRIPPENDORFF_ALPHA_AGREEMENT,
                    TTLabCodingIAACollectionProcessingEngine.PARAM_SET_SELECTION_STRATEGY, SetSelectionStrategy.MAX,
                    TTLabCodingIAACollectionProcessingEngine.PARAM_MULTI_CAS_HANDLING, TTLabCodingIAACollectionProcessingEngine.BOTH,
                    TTLabCodingIAACollectionProcessingEngine.PARAM_MIN_ANNOTATIONS, 10,
                    TTLabCodingIAACollectionProcessingEngine.PARAM_TARGET_LOCATION, "src/test/resources/out/coding/"
            ));
//			ab.add(AnalysisEngineFactory.createEngineDescription(
//					TTLabCodingIAACollectionProcessingEngine.class,
//					TTLabCodingIAACollectionProcessingEngine.PARAM_ANNOTATION_CLASSES, annotationClasses,
//					TTLabCodingIAACollectionProcessingEngine.PARAM_INCLUDE_FLAGS, codingIncludeFlags,
//					TTLabCodingIAACollectionProcessingEngine.PARAM_MIN_VIEWS, 2,
//					TTLabCodingIAACollectionProcessingEngine.PARAM_ANNOTATOR_LIST, annotatorWhitelist,
//					TTLabCodingIAACollectionProcessingEngine.PARAM_ANNOTATOR_RELATION, TTLabCodingIAACollectionProcessingEngine.WHITELIST,
//					TTLabCodingIAACollectionProcessingEngine.PARAM_FILTER_FINGERPRINTED, filterFingerprinted,
//					TTLabCodingIAACollectionProcessingEngine.PARAM_AGREEMENT_MEASURE, TTLabCodingIAACollectionProcessingEngine.PercentageAgreement,
//					TTLabCodingIAACollectionProcessingEngine.PARAM_SET_SELECTION_STRATEGY, SetSelectionStrategy.MAX,
//					TTLabCodingIAACollectionProcessingEngine.PARAM_MULTI_CAS_HANDLING, TTLabCodingIAACollectionProcessingEngine.BOTH
//			));
//			ab.add(AnalysisEngineFactory.createEngineDescription(
//					TTLabCodingIAACollectionProcessingEngine.class,
//					TTLabCodingIAACollectionProcessingEngine.PARAM_ANNOTATION_CLASSES, annotationClasses,
//					TTLabCodingIAACollectionProcessingEngine.PARAM_INCLUDE_FLAGS, codingIncludeFlags,
//					TTLabCodingIAACollectionProcessingEngine.PARAM_MIN_VIEWS, 2,
//					TTLabCodingIAACollectionProcessingEngine.PARAM_ANNOTATOR_LIST, annotatorWhitelist,
//					TTLabCodingIAACollectionProcessingEngine.PARAM_ANNOTATOR_RELATION, TTLabCodingIAACollectionProcessingEngine.WHITELIST,
//					TTLabCodingIAACollectionProcessingEngine.PARAM_FILTER_FINGERPRINTED, filterFingerprinted,
//					TTLabCodingIAACollectionProcessingEngine.PARAM_AGREEMENT_MEASURE, TTLabCodingIAACollectionProcessingEngine.FleissKappaAgreement,
//					TTLabCodingIAACollectionProcessingEngine.PARAM_SET_SELECTION_STRATEGY, SetSelectionStrategy.ALL,
//					TTLabCodingIAACollectionProcessingEngine.PARAM_MULTI_CAS_HANDLING, TTLabCodingIAACollectionProcessingEngine.BOTH
//			));

            SimplePipeline.runPipeline(collection, ab.createAggregate());
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
        System.out.println("\nDone");
    }
}
