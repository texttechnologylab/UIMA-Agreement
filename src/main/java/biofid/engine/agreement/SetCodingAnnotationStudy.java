package biofid.engine.agreement;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.dkpro.statistics.agreement.coding.CodingAnnotationStudy;
import org.dkpro.statistics.agreement.coding.ICodingAnnotationItem;
import org.dkpro.statistics.agreement.coding.KrippendorffAlphaAgreement;
import org.dkpro.statistics.agreement.distance.NominalDistanceFunction;
import org.jetbrains.annotations.NotNull;

import java.util.*;

public class SetCodingAnnotationStudy extends CodingAnnotationStudy {
	
	private SetSelectionStrategy setSelectionStrategy = SetSelectionStrategy.MAX;
	private final KrippendorffAlphaAgreement krippendorffAlphaAgreement;
	private final CodingAnnotationStudy dummyStudy;
	private final Comparator<List<String>> sortByAgreement;
	
	public SetCodingAnnotationStudy() {
		super();
		dummyStudy = new CodingAnnotationStudy(2);
		krippendorffAlphaAgreement = new KrippendorffAlphaAgreement(dummyStudy, new NominalDistanceFunction());
		sortByAgreement = Comparator.comparingDouble(this::getItemAgreement);
	}
	
	public SetCodingAnnotationStudy(int raterCount) {
		super(raterCount);
		dummyStudy = new CodingAnnotationStudy(raterCount);
		krippendorffAlphaAgreement = new KrippendorffAlphaAgreement(dummyStudy, new NominalDistanceFunction());
		sortByAgreement = Comparator.comparingDouble(this::getItemAgreement);
	}
	
	public SetCodingAnnotationStudy(SetSelectionStrategy strategy) {
		super();
		setSelectionStrategy = strategy;
		dummyStudy = new CodingAnnotationStudy(2);
		krippendorffAlphaAgreement = new KrippendorffAlphaAgreement(dummyStudy, new NominalDistanceFunction());
		sortByAgreement = Comparator.comparingDouble(this::getItemAgreement);
	}
	
	public SetCodingAnnotationStudy(int raterCount, SetSelectionStrategy strategy) {
		super(raterCount);
		dummyStudy = new CodingAnnotationStudy(raterCount);
		setSelectionStrategy = strategy;
		krippendorffAlphaAgreement = new KrippendorffAlphaAgreement(dummyStudy, new NominalDistanceFunction());
		sortByAgreement = Comparator.comparingDouble(this::getItemAgreement);
	}
	
	public ICodingAnnotationItem[] addItemSets(Set<String>... annotations) {
		return addItemSetsAsArray(annotations);
	}
	
	public ICodingAnnotationItem[] addItemSetsAsArray(Set<String>[] annotations) {
		Set<List<String>> cartesianProduct = Sets.cartesianProduct(Lists.newArrayList(annotations));
		ArrayList<ICodingAnnotationItem> items = new ArrayList<>();
		switch (setSelectionStrategy) {
			case ALL:
				cartesianProduct.forEach(item -> items.add(this.addItemAsArray(getAnnotations(item))));
				return items.toArray(new ICodingAnnotationItem[0]);
			case MAX:
			default:
				TreeSet<List<String>> treeSet = new TreeSet<>(sortByAgreement);
				treeSet.addAll(cartesianProduct);
				List<String> last = treeSet.last();
				ICodingAnnotationItem maxAgreementItem = this.addItemAsArray(getAnnotations(last));
				return new ICodingAnnotationItem[]{maxAgreementItem};
			case MATCH:
				ArrayList<HashSet<String>> annotationSets = new ArrayList<>();
				HashSet<String> allAnnotations = Sets.newHashSet();
				for (Set<String> stringSet : annotations) {
					allAnnotations.addAll(stringSet);
					annotationSets.add(Sets.newHashSet(stringSet));
				}
				for (String annotation : allAnnotations) {
					ArrayList<String> item = new ArrayList<>();
					for (int i = 0; i < annotationSets.size(); i++) item.add("");
					
					for (int i = 0; i < annotationSets.size(); i++) {
						HashSet<String> annotationSet = annotationSets.get(i);
						if (annotationSet.contains(annotation)) item.set(i, annotation);
						annotationSet.remove(annotation);
					}
					items.add(this.addItemAsArray(getAnnotations(item)));
				}
				return items.toArray(new ICodingAnnotationItem[0]);
		}
	}
	
	/**
	 * Returns an array of not empty or null annotations for a given set of category strings.
	 * If a category string of annotator A is <b>null</b> or empty, it is replaced with the string "{idx(A)}\<null\>".
	 *
	 * @param strings A list of category strings.
	 * @return An array of not empty or null annotations.
	 */
	@NotNull
	private String[] getAnnotations(List<String> strings) {
		String[] annotations = strings.toArray(new String[0]);
		for (int i = 0; i < strings.size(); i++) {
			if (StringUtils.isEmpty(strings.get(i)))
				annotations[i] = i + "<null>";
		}
		return annotations;
	}
	
	/**
	 * Calculate the agreement for a given unit.
	 *
	 * @param strings The category strings of the given unit.
	 * @return The agreement value.
	 */
	private double getItemAgreement(final List<String> strings) {
		return krippendorffAlphaAgreement.calculateItemAgreement(dummyStudy.addItemAsArray(getAnnotations(strings)));
	}
}

