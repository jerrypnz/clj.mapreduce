(ns clj.mapreduce.core-test
  (:use clojure.test
        clj.mapreduce.core)
  (:require [clojure.string :as str])
  (:import (java.io StringReader
                    BufferedReader)))

(defn word-count-map 
  "Test word count map function"
  [line]
  (->> (str/split line #"[\s\,\.\-\"\(\)]+")
       (map #(.toLowerCase %))
       (remove #{"" "a" "for" "the" "an" "of" "to"})
       (map (fn [word] [word 1]))))

(defn word-count-reduce
  "Test word count reduce function"
  [k values]
  (reduce + values))

(deftest word-count-test
  (testing "Map reduce basic function test via word count demo"
    (let [article "The quick
                   fox jumps over
                   the lazy dog,
                   blah blah blah...
                   The quick fox jumps over
                   the lazy dog.
                   Hello world to you, blah, blah..."
          expected-result {"quick" 2
                           "fox" 2
                           "dog" 2
                           "blah" 5
                           "jumps" 2
                           "over" 2
                           "lazy" 2
                           "hello" 1
                           "world" 1
                           "you" 1}
          input-seq (line-seq (BufferedReader. (StringReader. article)))
          actual-result (binding [*chunk-size* 2]
                          (into {}
                                (map-reduce! input-seq
                                             :map word-count-map
                                             :reduce word-count-reduce)))]
      (println actual-result)
      (is (= expected-result
             actual-result)))))