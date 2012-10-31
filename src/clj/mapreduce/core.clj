(ns clj.mapreduce.core
  (:use clj.mapreduce.storage))

(defn- group-reduce 
  "Grouping and then reducing"
  [group-fn reduce-fn coll]
  (->> coll
       (group-by group-fn)
       (map (fn [[k kvs]]
              [k (reduce-fn k (map second kvs))]))))

(def ^:dynamic *chunk-size* 10000)

(defmacro pmapcat
  [f coll]
  `(apply concat
          (pmap ~f ~coll)))

(defn- core-map-reduce
  "Core map/reduce function. Run map/reduce on given input sequence
  with given map function and reduce function. Returns a seq of
  the result key-value pairs."
  [input-seq & {map-fn :map
                reduce-fn :reduce}]
  (let [chunks (partition-all *chunk-size* input-seq)]
    (->> chunks
         (pmapcat (fn [chunk]
                    (->> chunk
                         (mapcat map-fn)
                         (group-reduce first reduce-fn))))
         (group-reduce first reduce-fn))))

(defn- merge-result 
  "Merge two maps. If a reduce-fn is not provided, `clojure.core/merge`
  is called. Otherwise, the two maps are merged with given function, like
  `clojure.core/merge-with` does, but the arguments of the function is not
  the same: this version uses the key as the first argument and a vector
  of values as the second argument."
  [m1 m2 & [reduce-fn]]
  (if reduce-fn
    (merge m1 m2)
    (persistent! (reduce (fn [m [k v]]
                           (if-let [v1 (get m k)]
                             (assoc! m k (reduce-fn k [v1 v]))
                             (assoc! m k v)))
                         (transient m1)
                         m2))))

(defn map-reduce!
  "Run a map/reduce job on given input with given map
  function and reduce function, output the result to
  the given storage. If the output storage already
  exists, the user may choose to :replace existing values
  with new values, or :reduce them."
  [input-seq & {map-fn :map
                reduce-fn :reduce
                output :out
                merge-strategy :merge-strategy}]
  (let [result (core-map-reduce input-seq
                                :map map-fn
                                :reduce reduce-fn)]
    (if output
      (let [existing-result (read! output)
            result (if (= merge-strategy :reduce)
                     (merge-result existing-result result reduce-fn)
                     (merge-result existing-result result))]
        (save! output result))
      result)))