/* Generic definitions */
/* Assertions (useful to generate conditional code) */
/* Current type and class (and size, if applicable) */
/* Value methods */
/* Interfaces (keys) */
/* Interfaces (values) */
/* Abstract implementations (keys) */
/* Abstract implementations (values) */
/* Static containers (keys) */
/* Static containers (values) */
/* Implementations */
/* Synchronized wrappers */
/* Unmodifiable wrappers */
/* Other wrappers */
/* Methods (keys) */
/* Methods (values) */
/* Methods (keys/values) */
/* Methods that have special names depending on keys (but the special names depend on values) */
/* Equality */
/* Object/Reference-only definitions (keys) */
/* Primitive-type-only definitions (keys) */
/* Object/Reference-only definitions (values) */
/* Primitive-type-only definitions (values) */
/*		 
 * Copyright (C) 2002-2014 Sebastiano Vigna 
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. 
 */
package it.unimi.dsi.fastutil.ints;
import it.unimi.dsi.fastutil.ints.IntCollection;
import it.unimi.dsi.fastutil.objects.ObjectSet;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import java.util.Map;
/** A type-specific {@link Map}; provides some additional methods that use polymorphism to avoid (un)boxing, and handling of a default return value.
 *
 * <P>Besides extending the corresponding type-specific {@linkplain it.unimi.dsi.fastutil.Function function}, this interface strengthens {@link #entrySet()},
 * {@link #keySet()} and {@link #values()}. Maps returning entry sets of type {@link FastEntrySet} support also fast iteration.
 *
 * <P>A submap or subset may or may not have an
 * independent default return value (which however must be initialized to the
 * default return value of the originator).
 *
 * @see Map
 */
public interface Int2IntMap extends Int2IntFunction , Map<Integer,Integer> {
 /** An entry set providing fast iteration. 
	 *
	 * <p>In some cases (e.g., hash-based classes) iteration over an entry set requires the creation
	 * of a large number of {@link java.util.Map.Entry} objects. Some <code>fastutil</code>
	 * maps might return {@linkplain #entrySet() entry set} objects of type <code>FastEntrySet</code>: in this case, {@link #fastIterator() fastIterator()}
	 * will return an iterator that is guaranteed not to create a large number of objects, <em>possibly
	 * by returning always the same entry</em> (of course, mutated).
	 */
 public interface FastEntrySet extends ObjectSet<Int2IntMap.Entry > {
  /** Returns a fast iterator over this entry set; the iterator might return always the same entry object, suitably mutated.
		 *
		 * @return a fast iterator over this entry set; the iterator might return always the same {@link java.util.Map.Entry} object, suitably mutated.
		 */
  public ObjectIterator<Int2IntMap.Entry > fastIterator();
 }
 /** Returns a set view of the mappings contained in this map.
	 *  <P>Note that this specification strengthens the one given in {@link Map#entrySet()}.
	 *
	 * @return a set view of the mappings contained in this map.
	 * @see Map#entrySet()
	 */
 ObjectSet<Map.Entry<Integer, Integer>> entrySet();
 /** Returns a type-specific set view of the mappings contained in this map.
	 *
	 * <p>This method is necessary because there is no inheritance along
	 * type parameters: it is thus impossible to strengthen {@link #entrySet()}
	 * so that it returns an {@link it.unimi.dsi.fastutil.objects.ObjectSet}
	 * of objects of type {@link java.util.Map.Entry} (the latter makes it possible to
	 * access keys and values with type-specific methods).
	 *
	 * @return a type-specific set view of the mappings contained in this map.
	 * @see #entrySet()
	 */
 ObjectSet<Int2IntMap.Entry > int2IntEntrySet();
 /** Returns a set view of the keys contained in this map.
	 *  <P>Note that this specification strengthens the one given in {@link Map#keySet()}.
	 *
	 * @return a set view of the keys contained in this map.
	 * @see Map#keySet()
	 */
 IntSet keySet();
 /** Returns a set view of the values contained in this map.
	 *  <P>Note that this specification strengthens the one given in {@link Map#values()}.
	 *
	 * @return a set view of the values contained in this map.
	 * @see Map#values()
	 */
 IntCollection values();
 /**
	 * @see Map#containsValue(Object)
	 */
 boolean containsValue( int value );
 /** A type-specific {@link java.util.Map.Entry}; provides some additional methods
	 *  that use polymorphism to avoid (un)boxing.
	 *
	 * @see java.util.Map.Entry
	 */
 interface Entry extends Map.Entry <Integer,Integer> {
  /**
		 * @see java.util.Map.Entry#getKey()
		 */
  int getIntKey();
  /**
		 * @see java.util.Map.Entry#setValue(Object)
		 */
  int setValue(int value);
  /**
		 * @see java.util.Map.Entry#getValue()
		 */
  int getIntValue();
 }
}
