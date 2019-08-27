package biofid.engine.agreement;

import com.google.common.collect.Lists;
import org.apache.commons.io.FileUtils;
import org.apache.uima.analysis_engine.AnalysisEngine;
import org.apache.uima.fit.factory.AnalysisEngineFactory;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.fit.pipeline.SimplePipeline;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.apache.uima.util.CasIOUtils;
import org.junit.jupiter.api.Test;
import org.texttechnologylab.annotation.AbstractNamedEntity;
import org.texttechnologylab.annotation.NamedEntity;
import org.texttechnologylab.iaa.Agreement;

import java.io.File;
import java.io.FileInputStream;

/**
 * Created on 28.01.2019.
 */
public class AgreementAnnotatorTest {
	@Test
	public void testAnnotatorAgreement() {
		try {
			String[] annotators = {"305236", "305235"};
			String xmiPath = "src/test/out/xmi/";
			
			JCas jCas = JCasFactory.createJCas();
			try (FileInputStream inputStream = FileUtils.openInputStream(new File(xmiPath + "3713524.xmi"))) {
				CasIOUtils.load(inputStream, null, jCas.getCas(), true);
				
				
				// Test parameters
				boolean filterFingerprinted = false;
				String[] annotationClasses = {NamedEntity.class.getName(), AbstractNamedEntity.class.getName()};
				
				AnalysisEngine allEngine = AnalysisEngineFactory.createEngine(
						AgreementAnnotatorEngine.class,
						AgreementAnnotatorEngine.PARAM_ANNOTATION_CLASSES, annotationClasses,
						AgreementAnnotatorEngine.PARAM_MIN_VIEWS, 2,
						AgreementAnnotatorEngine.PARAM_ANNOTATOR_LIST, annotators,
						AgreementAnnotatorEngine.PARAM_ANNOTATOR_RELATION, AgreementAnnotatorEngine.WHITELIST,
						AgreementAnnotatorEngine.PARAM_FILTER_FINGERPRINTED, filterFingerprinted,
						AgreementAnnotatorEngine.PARAM_AGREEMENT_MEASURE, AgreementAnnotatorEngine.KrippendorffAlphaAgreement,
						AgreementAnnotatorEngine.PARAM_SET_SELECTION_STRATEGY, SetSelectionStrategy.MATCH,
						AgreementAnnotatorEngine.PARAM_PRINT_STATS, false
				);
				
				SimplePipeline.runPipeline(jCas, allEngine);
				System.out.println("Token\tAgreement");
				JCas iaa = jCas.getView("IAA");
				Lists.newArrayList(JCasUtil.select(iaa, Agreement.class)).subList(0, 100)
						.forEach(agreement -> System.out.printf("%s\t%f\n", agreement.getCoveredText(), agreement.getAgreementValue()));
				System.out.println();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("\nDone");
	}
}
