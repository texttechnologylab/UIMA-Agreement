package biofid.engine.agreement.annotation;

import org.apache.uima.jcas.JCas;
import org.apache.uima.jcas.cas.TOP_Type;
import org.apache.uima.jcas.tcas.Annotation;

public class Agreement extends Annotation {
	
	private Double agreementValue;
	private String agreementMeasure;
	
	public Agreement() {
	}
	
	public Agreement(int addr, TOP_Type type) {
		super(addr, type);
	}
	
	public Agreement(JCas jcas) {
		super(jcas);
	}
	
	public Agreement(JCas jcas, int begin, int end) {
		super(jcas, begin, end);
	}
	
	public Double getAgreementValue() {
		return agreementValue;
	}
	
	public void setAgreementValue(Double agreementValue) {
		this.agreementValue = agreementValue;
	}
	
	public String getAgreementMeasure() {
		return agreementMeasure;
	}
	
	public void setAgreementMeasure(String agreementMeasure) {
		this.agreementMeasure = agreementMeasure;
	}
}
