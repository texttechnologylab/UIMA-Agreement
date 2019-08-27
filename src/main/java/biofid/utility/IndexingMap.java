package biofid.utility;

import org.apache.commons.collections4.bidimap.DualLinkedHashBidiMap;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * An automatically indexing map backed with a {@link DualLinkedHashBidiMap} that keeps a running index for its keys.
 * The index is backed by {@link AtomicInteger} and the index increment thus tread safe.
 *
 * @param <T> The key type.
 */
public class IndexingMap<T> extends DualLinkedHashBidiMap<T, Integer> {
	private AtomicInteger index;
	
	/**
	 * Indices start with 0 by default.
	 */
	public IndexingMap() {
		index = new AtomicInteger(0);
	}
	
	/**
	 * Indices start with 'begin'.
	 *
	 * @param begin The first index to use.
	 */
	public IndexingMap(int begin) {
		index = new AtomicInteger(begin);
	}
	
	/**
	 * Add a key-value-pair (item, index) if the item is absent from the key set
	 * and increase the running index afterwards.
	 *
	 * @param item The key to add to the indexing map.
	 */
	public void add(T item) {
		if (!this.containsKey(item)) {
			super.put(item, index.getAndIncrement());
		}
	}
	
	/**
	 * Be careful when using this, as the running index is not changed.
	 * Use {@link IndexingMap#add(Object) add} instead.
	 */
	@Deprecated
	public Integer put(T t, Integer integer) {
		return super.put(t, integer);
	}
}
