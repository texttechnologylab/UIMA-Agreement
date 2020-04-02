package org.texttechnologylab.agreement.engine;

import com.google.common.collect.Lists;
import org.apache.commons.io.FileUtils;
import org.apache.uima.analysis_engine.AnalysisEngine;
import org.apache.uima.fit.factory.AnalysisEngineFactory;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.fit.pipeline.SimplePipeline;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.apache.uima.jcas.cas.DoubleArray;
import org.apache.uima.jcas.cas.LongArray;
import org.apache.uima.jcas.cas.StringArray;
import org.apache.uima.util.CasIOUtils;
import org.junit.Test;
import org.texttechnologylab.annotation.AbstractNamedEntity;
import org.texttechnologylab.annotation.NamedEntity;
import org.texttechnologylab.iaa.Agreement;
import org.texttechnologylab.iaa.AgreementContainer;

import java.io.File;
import java.io.FileInputStream;
import java.util.Collection;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Created on 28.01.2019.
 */
public class AgreementAnnotatorTest {
    @Test
    public void testAnnotatorAgreement() {
        try {
            String[] annotatorWhitelist = {"305236", "305235"};
            String[] annotatorBlacklist = {"0", "302904"};
            String xmiPath = "src/test/resources/out/xmi/";

            JCas jCas = JCasFactory.createJCas();

            // Test parameters
            boolean filterFingerprinted = false;
            String[] annotationClasses = {NamedEntity.class.getName(), AbstractNamedEntity.class.getName()};

            AnalysisEngine codingAnnotatorEngine = AnalysisEngineFactory.createEngine(
                    CodingAgreementAnnotatorEngine.class,
                    CodingAgreementAnnotatorEngine.PARAM_ANNOTATION_CLASSES, annotationClasses,
                    CodingAgreementAnnotatorEngine.PARAM_MIN_VIEWS, 2,
//						AgreementAnnotatorEngine.PARAM_ANNOTATOR_LIST, annotatorWhitelist,
//						AgreementAnnotatorEngine.PARAM_ANNOTATOR_RELATION, AgreementAnnotatorEngine.WHITELIST,
                    CodingAgreementAnnotatorEngine.PARAM_ANNOTATOR_LIST, annotatorBlacklist,
                    CodingAgreementAnnotatorEngine.PARAM_ANNOTATOR_RELATION, CodingAgreementAnnotatorEngine.BLACKLIST,
                    CodingAgreementAnnotatorEngine.PARAM_FILTER_FINGERPRINTED, filterFingerprinted,
                    CodingAgreementAnnotatorEngine.PARAM_AGREEMENT_MEASURE, CodingAgreementAnnotatorEngine.KRIPPENDORFF_ALPHA_AGREEMENT,
                    CodingAgreementAnnotatorEngine.PARAM_SET_SELECTION_STRATEGY, SetSelectionStrategy.MAX,
                    CodingAgreementAnnotatorEngine.PARAM_PRINT_STATS, false
            );

            AnalysisEngine unitizingAnnotatorEngine = AnalysisEngineFactory.createEngine(
                    UnitizingAgreementAnnotatorEngine.class,
                    UnitizingAgreementAnnotatorEngine.PARAM_ANNOTATION_CLASSES, annotationClasses,
                    UnitizingAgreementAnnotatorEngine.PARAM_MIN_VIEWS, 2,
//						UnitizingAgreementAnnotatorEngine.PARAM_ANNOTATOR_LIST, annotatorWhitelist,
//						UnitizingAgreementAnnotatorEngine.PARAM_ANNOTATOR_RELATION, AgreementAnnotatorEngine.WHITELIST,
                    UnitizingAgreementAnnotatorEngine.PARAM_ANNOTATOR_LIST, annotatorBlacklist,
                    UnitizingAgreementAnnotatorEngine.PARAM_ANNOTATOR_RELATION, CodingAgreementAnnotatorEngine.BLACKLIST,
                    UnitizingAgreementAnnotatorEngine.PARAM_FILTER_FINGERPRINTED, filterFingerprinted,
                    UnitizingAgreementAnnotatorEngine.PARAM_PRINT_STATS, false
            );
            try (FileInputStream inputStream = FileUtils.openInputStream(new File(xmiPath + "3713524.xmi"))) {
                CasIOUtils.load(inputStream, null, jCas.getCas(), true);

                System.out.println("UnitizingAgreementAnnotatorEngine");
                SimplePipeline.runPipeline(jCas, unitizingAnnotatorEngine);

                JCas iaa = jCas.getView("IAA");
                assertTrue(printCategoryAgreement(iaa));

                System.out.println("CodingAgreementAnnotatorEngine");
                SimplePipeline.runPipeline(jCas, codingAnnotatorEngine);

                iaa = jCas.getView("IAA");
                assertTrue(printCategoryAgreement(iaa));

                System.out.println("Token\tAgreement");
                Collection<Agreement> agreementCollection = JCasUtil.select(iaa, Agreement.class);
                assertFalse(agreementCollection.isEmpty());
                Lists.newArrayList(agreementCollection).subList(0, 100)
                        .forEach(agreement -> System.out.printf("%s\t%f\n", agreement.getCoveredText(), agreement.getAgreementValue()));
                System.out.println();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("\nDone");
    }

    private boolean printCategoryAgreement(JCas iaa) {
        AgreementContainer agreementContainer = Lists.newArrayList(JCasUtil.select(iaa, AgreementContainer.class)).get(0);
        StringArray categoryNames = agreementContainer.getCategoryNames();
        DoubleArray categoryAgreementValues = agreementContainer.getCategoryAgreementValues();
        LongArray categoryCounts = agreementContainer.getCategoryCounts();

        long totalCount = 0;

        System.out.println("Category\tAgreement");
        System.out.printf("Overall\t%f\n", agreementContainer.getOverallAgreementValue());
        for (int i = 0; i < categoryNames.size(); i++) {
            String category = categoryNames.get(i);
            Double value = categoryAgreementValues.get(i);
            long count = categoryCounts.get(i);
            totalCount += count;
            System.out.printf("%s\t%d\t%f\n", category, count, value);
        }
        System.out.println();

        return totalCount > 0;
    }
}
