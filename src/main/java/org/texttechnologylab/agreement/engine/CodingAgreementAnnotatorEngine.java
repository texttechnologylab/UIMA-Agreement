package org.texttechnologylab.agreement.engine;

import com.google.common.collect.ImmutableSet;
import eu.openminted.share.annotations.api.Parameters;
import org.apache.uima.UimaContext;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.resource.ResourceInitializationException;
import org.dkpro.statistics.agreement.ICategorySpecificAgreement;
import org.dkpro.statistics.agreement.coding.CodingAnnotationStudy;
import org.dkpro.statistics.agreement.coding.ICodingItemSpecificAgreement;
import org.dkpro.statistics.agreement.coding.KrippendorffAlphaAgreement;
import org.dkpro.statistics.agreement.coding.PercentageAgreement;

import static org.texttechnologylab.agreement.engine.AbstractIAAEngine.PARAM_MULTI_CAS_HANDLING;
import static org.texttechnologylab.agreement.engine.CodingIAACollectionProcessingEngine.PARAM_ANNOTATE_TOKEN;

/**
 * Inter-annotator agreement engine using a {@link CodingAnnotationStudy CodingAnnotationStudy} and {@link
 * ICategorySpecificAgreement ICategorySpecificAgreement} measure.
 * <p/>
 * Convenience class for separate CAS annotation with empty {@link org.apache.uima.analysis_engine.AnalysisEngine#collectionProcessComplete
 * collectionProcessComplete()}.
 * <br>
 * {@link CodingAgreementAnnotatorEngine#PARAM_MULTI_CAS_HANDLING PARAM_MULTI_CAS_HANDLING} is fixed to {@link
 * CodingAgreementAnnotatorEngine#SEPARATE SEPARATE} and {@link CodingAgreementAnnotatorEngine#PARAM_ANNOTATE_TOKEN
 * PARAM_ANNOTATE} is fixed to 'true'.
 * <p/>
 * <b>IMPORTANT:</b>
 * Requires chosen agreement measure to implement interface {@link ICodingItemSpecificAgreement}!
 * <br>
 * Currently supported measures:
 * <ul>
 *     <li>{@link CodingIAACollectionProcessingEngine#KRIPPENDORFF_ALPHA_AGREEMENT CodingInterAnnotatorAgreementEngine.KrippendorffAlphaAgreement}
 *     <li>{@link CodingIAACollectionProcessingEngine#PERCENTAGE_AGREEMENT CodingInterAnnotatorAgreementEngine.PercentageAgreement}
 * </ul>
 *
 * @see ICodingItemSpecificAgreement
 * @see KrippendorffAlphaAgreement
 * @see PercentageAgreement
 */

@Parameters(
        exclude = {
                PARAM_MULTI_CAS_HANDLING,
                PARAM_ANNOTATE_TOKEN
        })
public class CodingAgreementAnnotatorEngine extends CodingIAACollectionProcessingEngine {
    @Override
    public void initialize(UimaContext context) throws ResourceInitializationException {
        super.initialize(context);
        pAnnotateToken = true;
        pAnnotateDocument = true;
        pMultiCasHandling = SEPARATE;
        if (!(ImmutableSet.of(KRIPPENDORFF_ALPHA_AGREEMENT, PERCENTAGE_AGREEMENT).contains(pAgreementMeasure))) {
            throw new ResourceInitializationException(new UnsupportedOperationException(
                    "PARAM_ANNOTATE is set 'true', but the chosen PARAM_AGREEMENT_MEASURE does not implement ICodingItemSpecificAgreement!"
            ));
        }
    }

    @Override
    public void collectionProcessComplete() throws AnalysisEngineProcessException {
        // Empty
    }
}
