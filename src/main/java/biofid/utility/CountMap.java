package biofid.utility;

import java.util.Collection;
import java.util.HashMap;

public class CountMap<T> extends HashMap<T, Long> {
	
	/**
	 * Add the key to the CountMap. This will not affect keys already added.
	 *
	 * @param key
	 */
	public void add(T key) {
		this.put(key, this.getOrDefault(key, 0L));
	}
	
	/**
	 * Increase the count for this key. If the key does not exist, it will be added.
	 *
	 * @param key
	 */
	public void inc(T key) {
		this.put(key, this.getOrDefault(key, 0L) + 1L);
	}
	
	/**
	 * Increase the count for each of these keys. If any key does not exist, it will be added.
	 *
	 * @param keys
	 */
	public void incAll(Collection<T> keys) {
		for (T key : keys) {
			this.put(key, this.getOrDefault(key, 0L) + 1L);
		}
	}
	
	/**
	 * Get the count for this key, or 0 if it does not exist.
	 *
	 * @param key
	 * @return
	 */
	public Long get(Object key) {
		return this.getOrDefault(key, 0L);
	}
	
	/**
	 * Get the minimum value in this map.
	 *
	 * @return the minimum.
	 */
	public Long min() {
		return this.values().stream().min(Long::compare).orElse(0L);
	}
	
	/**
	 * Get the maximum value in this map.
	 *
	 * @return the maximum.
	 */
	public Long max() {
		return this.values().stream().max(Long::compare).orElse(0L);
	}
}
