package org.hucompute.textimager.uima.agreement.engine;

import org.apache.uima.collection.CollectionReader;
import org.apache.uima.fit.factory.AggregateBuilder;
import org.apache.uima.fit.factory.AnalysisEngineFactory;
import org.apache.uima.fit.factory.CollectionReaderFactory;
import org.apache.uima.fit.pipeline.SimplePipeline;
import org.dkpro.core.io.xmi.XmiReader;
import org.hucompute.textimager.uima.agreement.engine.relational.RelationAnnotationAgreement;
import org.hucompute.textimager.uima.agreement.engine.unitizing.UnitizingIAACollectionProcessingEngine;
import org.junit.jupiter.api.Test;
import org.texttechnologylab.annotation.AbstractNamedEntity;
import org.texttechnologylab.annotation.NamedEntity;

public class RelationAgreementTest {
    @Test
    public void testAnnotatorAgreement() {
        try {
            final boolean download = false;

            String xmiPath = System.getenv("HOME") + "/BIOfid/data/export/TA-2021.02.17/";
            CollectionReader collection  = CollectionReaderFactory.createReader(
                        XmiReader.class,
                    XmiReader.PARAM_PATTERNS, new String[]{"[+]3673151.xmi", "[+]4497041.xmi"},
                        XmiReader.PARAM_SOURCE_LOCATION, xmiPath,
                        XmiReader.PARAM_LENIENT, true
//						, XmiReader.PARAM_LOG_FREQ, -1
                );

            AggregateBuilder ab = new AggregateBuilder();

            // Test parameters
            String[] annotatorWhitelist = {"306513", "306320"};  // Marc & Andy
//            String[] annotatorBlacklist = {"0", "302904", "303228", "306320", "305718", "306513"};
//            String[] annotatorBlacklist = {"0"};
            boolean filterFingerprinted = true;
            String[] annotationClasses = {NamedEntity.class.getName(), AbstractNamedEntity.class.getName()};

            ab.add(AnalysisEngineFactory.createEngineDescription(RelationAnnotationAgreement.class,
                    RelationAnnotationAgreement.PARAM_MIN_VIEWS, 1,
					RelationAnnotationAgreement.PARAM_ANNOTATOR_LIST, annotatorWhitelist,
					RelationAnnotationAgreement.PARAM_ANNOTATOR_RELATION, UnitizingIAACollectionProcessingEngine.WHITELIST,
//                    RelationAnnotationAgreement.PARAM_ANNOTATOR_LIST, annotatorBlacklist,
//                    RelationAnnotationAgreement.PARAM_ANNOTATOR_RELATION, UnitizingIAACollectionProcessingEngine.BLACKLIST,
                    RelationAnnotationAgreement.PARAM_FILTER_FINGERPRINTED, filterFingerprinted,
                    RelationAnnotationAgreement.PARAM_MULTI_CAS_HANDLING, RelationAnnotationAgreement.BOTH,
                    RelationAnnotationAgreement.PARAM_PRINT_STATS, true,
					RelationAnnotationAgreement.PARAM_FILTER_PROCESSED, true,
                    RelationAnnotationAgreement.PARAM_TARGET_LOCATION, "src/test/resources/out/relational"
            ));
            SimplePipeline.runPipeline(collection, ab.createAggregate());
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("\nDone");
    }
}
