package org.biofid.agreement.engine;

import eu.openminted.share.annotations.api.Parameters;
import org.apache.uima.UimaContext;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.resource.ResourceInitializationException;
import org.dkpro.statistics.agreement.ICategorySpecificAgreement;
import org.dkpro.statistics.agreement.unitizing.KrippendorffAlphaUnitizingAgreement;

/**
 * Inter-annotator agreement engine using a {@link org.dkpro.statistics.agreement.unitizing.UnitizingAnnotationStudy UnitizingAnnotationStudy} and
 * {@link org.dkpro.statistics.agreement.unitizing.KrippendorffAlphaUnitizingAgreement KrippendorffAlphaUnitizingAgreement} measure.
 * <p/>
 * Convenience class for separate CAS annotation with empty {@link org.apache.uima.analysis_engine.AnalysisEngine#collectionProcessComplete collectionProcessComplete()}.
 * <br>
 * {@link UnitizingAgreementAnnotatorEngine#PARAM_MULTI_CAS_HANDLING PARAM_MULTI_CAS_HANDLING} is fixed to {@link UnitizingAgreementAnnotatorEngine#SEPARATE SEPARATE} and
 * {@link UnitizingAgreementAnnotatorEngine#PARAM_ANNOTATE_DOCUMENT PARAM_ANNOTATE} is fixed to 'true'.
 * <p/>
 *
 * @see ICategorySpecificAgreement
 * @see KrippendorffAlphaUnitizingAgreement
 */

@Parameters(
		exclude = {
				UnitizingIAACollectionProcessingEngine.PARAM_MULTI_CAS_HANDLING,
				UnitizingIAACollectionProcessingEngine.PARAM_ANNOTATE_DOCUMENT
		})
public class UnitizingAgreementAnnotatorEngine extends UnitizingIAACollectionProcessingEngine {
	@Override
	public void initialize(UimaContext context) throws ResourceInitializationException {
		super.initialize(context);
		pAnnotateDocument = true;
		pMultiCasHandling = SEPARATE;
	}
	
	@Override
	public void collectionProcessComplete() throws AnalysisEngineProcessException {
		// Empty
	}
}
