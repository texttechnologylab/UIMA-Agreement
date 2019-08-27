package biofid.engine.agreement;

import biofid.engine.ColumnPrinterEngine;
import biofid.engine.reader.TextAnnotatorRepositoryCollectionReader;
import de.tudarmstadt.ukp.dkpro.core.io.xmi.XmiReader;
import org.apache.uima.collection.CollectionReader;
import org.apache.uima.fit.factory.AggregateBuilder;
import org.apache.uima.fit.factory.AnalysisEngineFactory;
import org.apache.uima.fit.factory.CollectionReaderFactory;
import org.apache.uima.fit.pipeline.SimplePipeline;
import org.junit.jupiter.api.Test;
import org.texttechnologylab.annotation.AbstractNamedEntity;
import org.texttechnologylab.annotation.NamedEntity;

/**
 * Created on 28.01.2019.
 */
public class InterAnnotatorAgreementEngineTest {
	@Test
	public void testAnnotatorAgreement() {
		try {
//			String[] annotators = {"303228", "22442"};
//			String xmiPath = "src/test/resources/";
			String[] annotators = {"305236", "305235"};
			String xmiPath = "src/test/out/xmi/";
//			String txtPath = "src/test/out/txt/";
//			CollectionReader collection = CollectionReaderFactory.createReader(
//					TextAnnotatorRepositoryCollectionReader.class,
//					TextAnnotatorRepositoryCollectionReader.PARAM_SOURCE_LOCATION, xmiPath,
//					TextAnnotatorRepositoryCollectionReader.PARAM_TARGET_LOCATION, txtPath,
//					TextAnnotatorRepositoryCollectionReader.PARAM_SESSION_ID, "", // FIXME: add session id here
//					TextAnnotatorRepositoryCollectionReader.PARAM_FORCE_RESERIALIZE, true
////					, XmiReader.PARAM_LOG_FREQ, -1
//			);
			CollectionReader collection = CollectionReaderFactory.createReader(
					XmiReader.class,
					XmiReader.PARAM_PATTERNS, "[+]*.xmi",
//					XmiReader.PARAM_PATTERNS, "[+]3713524.xmi",
					XmiReader.PARAM_SOURCE_LOCATION, xmiPath,
					XmiReader.PARAM_LOG_FREQ, -1
			);
			
			AggregateBuilder ab = new AggregateBuilder();
			
			// Test parameters
			boolean filterFingerprinted = true;
			String[] annotationClasses = {NamedEntity.class.getName(), AbstractNamedEntity.class.getName()};
			
			ab.add(AnalysisEngineFactory.createEngineDescription(
					ColumnPrinterEngine.class,
					ColumnPrinterEngine.PARAM_TARGET_LOCATION, "/tmp/ttemp.txt",
					ColumnPrinterEngine.PARAM_MIN_VIEWS, 2,
					ColumnPrinterEngine.PARAM_ANNOTATOR_LIST, annotators,
					ColumnPrinterEngine.PARAM_ANNOTATOR_RELATION, ColumnPrinterEngine.WHITELIST,
					ColumnPrinterEngine.PARAM_FILTER_FINGERPRINTED, filterFingerprinted
			));
			
			String[] includeFlags = new String[]{TTLabUnitizingIAACollectionProcessingEngine.METAPHOR, TTLabUnitizingIAACollectionProcessingEngine.METONYM};
			ab.add(AnalysisEngineFactory.createEngineDescription(
					TTLabUnitizingIAACollectionProcessingEngine.class,
					TTLabUnitizingIAACollectionProcessingEngine.PARAM_ANNOTATION_CLASSES, annotationClasses,
					TTLabUnitizingIAACollectionProcessingEngine.PARAM_INCLUDE_FLAGS, includeFlags,
					TTLabUnitizingIAACollectionProcessingEngine.PARAM_MIN_VIEWS, 2,
					TTLabUnitizingIAACollectionProcessingEngine.PARAM_ANNOTATOR_LIST, annotators,
					TTLabUnitizingIAACollectionProcessingEngine.PARAM_ANNOTATOR_RELATION, UnitizingIAACollectionProcessingEngine.WHITELIST,
					TTLabUnitizingIAACollectionProcessingEngine.PARAM_FILTER_FINGERPRINTED, filterFingerprinted,
					TTLabUnitizingIAACollectionProcessingEngine.PARAM_MULTI_CAS_HANDLING, TTLabUnitizingIAACollectionProcessingEngine.BOTH
			));

//			includeFlags = new String[]{TTLabCodingIAACollectionProcessingEngine.METAPHOR, TTLabCodingIAACollectionProcessingEngine.METONYM};
//			ab.add(AnalysisEngineFactory.createEngineDescription(
//					TTLabCodingIAACollectionProcessingEngine.class,
//					TTLabCodingIAACollectionProcessingEngine.PARAM_ANNOTATION_CLASSES, annotationClasses,
//					TTLabCodingIAACollectionProcessingEngine.PARAM_INCLUDE_FLAGS, includeFlags,
//					TTLabCodingIAACollectionProcessingEngine.PARAM_MIN_VIEWS, 2,
//					TTLabCodingIAACollectionProcessingEngine.PARAM_ANNOTATOR_LIST, annotators,
//					TTLabCodingIAACollectionProcessingEngine.PARAM_ANNOTATOR_RELATION, TTLabCodingIAACollectionProcessingEngine.WHITELIST,
//					TTLabCodingIAACollectionProcessingEngine.PARAM_FILTER_FINGERPRINTED, filterFingerprinted,
//					TTLabCodingIAACollectionProcessingEngine.PARAM_AGREEMENT_MEASURE, TTLabCodingIAACollectionProcessingEngine.KrippendorffAlphaAgreement,
//					TTLabCodingIAACollectionProcessingEngine.PARAM_SET_SELECTION_STRATEGY, SetSelectionStrategy.MATCH,
//					TTLabCodingIAACollectionProcessingEngine.PARAM_MULTI_CAS_HANDLING, TTLabCodingIAACollectionProcessingEngine.BOTH
//			));
//			ab.add(AnalysisEngineFactory.createEngineDescription(
//					TTLabCodingIAACollectionProcessingEngine.class,
//					TTLabCodingIAACollectionProcessingEngine.PARAM_ANNOTATION_CLASSES, annotationClasses,
//					TTLabCodingIAACollectionProcessingEngine.PARAM_INCLUDE_FLAGS, includeFlags,
//					TTLabCodingIAACollectionProcessingEngine.PARAM_MIN_VIEWS, 2,
//					TTLabCodingIAACollectionProcessingEngine.PARAM_ANNOTATOR_LIST, annotators,
//					TTLabCodingIAACollectionProcessingEngine.PARAM_ANNOTATOR_RELATION, TTLabCodingIAACollectionProcessingEngine.WHITELIST,
//					TTLabCodingIAACollectionProcessingEngine.PARAM_FILTER_FINGERPRINTED, filterFingerprinted,
//					TTLabCodingIAACollectionProcessingEngine.PARAM_AGREEMENT_MEASURE, TTLabCodingIAACollectionProcessingEngine.KrippendorffAlphaAgreement,
//					TTLabCodingIAACollectionProcessingEngine.PARAM_SET_SELECTION_STRATEGY, SetSelectionStrategy.MAX,
//					TTLabCodingIAACollectionProcessingEngine.PARAM_MULTI_CAS_HANDLING, TTLabCodingIAACollectionProcessingEngine.BOTH
//			));
			
			SimplePipeline.runPipeline(collection, ab.createAggregate());
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("\nDone");
	}
}
