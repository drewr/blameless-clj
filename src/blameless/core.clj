(ns blameless.core
  (:require [cheshire.core :as json]
            [clj-http.client :as http]
            [clojure.data.csv :as csv]
            [flatland.ordered.map :refer [ordered-map]]
            [java-time.api :as time]))

(def MAX_INCIDENTS 100)

(defn hostname [org]
  (format "https://%s.blameless.io/api/v1/incidents" org))

(defn utc-time [epoch]
  (-> epoch
      time/instant
      (time/zoned-date-time "UTC")))

(defn local-time [epoch]
  (-> (utc-time epoch)
      (time/zoned-date-time (.getZone (time/system-clock)))))

(defn excel-time [epoch]
  (time/format "yyyy-MM-dd HH:mm:ss" (utc-time epoch)))

(defonce incidents*
  (memoize
   (fn
     ([tok org offset limit order-by]
      (incidents* tok org offset limit order-by nil))
     ([tok org offset limit order-by cache-key]
      (let [resp (-> (http/get
                      (hostname org)
                      {:headers {:authorization (str "Bearer " tok)}
                       :query-params {:offset offset
                                      :limit limit
                                      :order_by order-by}})
                     :body (json/decode true))
            more-items? (fn [r]
                          (let [p (:pagination r)
                                is (pos? (count (:incidents r)))]
                            (prn 'more-items? is (:pagination r))
                            is))
            new-offset (fn [r]
                         (+ (-> r :pagination :offset)
                            (-> r :pagination :limit)))
            new-limit (fn [r]
                        MAX_INCIDENTS)
            is (:incidents resp)]
        (prn 'incidents*
             {:offset offset
              :limit limit
              :order_by order-by
              :total-count (-> resp :pagination :count)
              :this-count (count is)})
        (lazy-seq
         (when (seq is)
           (cons (first is)
                 (concat
                  (rest is)
                  (incidents*
                   tok org
                   (new-offset resp)
                   (new-limit resp)
                   order-by
                   :NOT_IMPLEMENTED))))))))))

(def incidents
  (fn [tok org]
    (incidents* tok org 0 MAX_INCIDENTS "created")))

;; Mostly taken from leontalbot at
;; https://stackoverflow.com/a/48244002, thanks!
(defn write-csv
  "Takes a file (path, name and extension) and
   csv-data (vector of vectors with all values) and
   writes csv file."
  [file csv-data]
  (with-open [writer (clojure.java.io/writer file :append false)]
    (csv/write-csv writer csv-data)))

(defn maps->csv-data
  "Takes a collection of maps and returns csv-data
   (vector of vectors with all values)."
  [maps]
  (let [columns (-> maps first keys)
        headers (mapv name columns)
        rows (mapv #(mapv % columns) maps)]
    (into [headers] rows)))

(defn write-csv-from-maps
  "Takes a file (path, name and extension) and a collection of maps
   transforms data (vector of vectors with all values)
   writes csv file."
  [file maps]
  (->> maps maps->csv-data (write-csv file)))

(defn select-keys* [m paths]
  (into {} (map (fn [p]
                  (if (fn? (peek p))
                    (let [f (peek p)
                          all-but-the-fn (-> p reverse rest reverse)]
                      [(peek all-but-the-fn) (f (get-in m all-but-the-fn))])
                    [(peek p) (get-in m p)])))
        paths))


(defn creator-email [incident]
  (let [f #(= (:creator incident) (:_id %))]
    (-> (first (filter f (:team incident)))
        :profile
        :email)))

(defn enhance-incident [i]
  (assoc (ordered-map)
    :id (-> i :_id)
    :created-UTC (-> i :created :$date excel-time str)
    :type (-> i :type)
    :severity (-> i :severity)
    :creator (creator-email i)
    :seconds-to-resolve (-> i :time_to_resolution)
    :hours-to-resolve (int (/ (-> i :time_to_resolution) 3600))
    :days-to-resolve (int (/ (-> i :time_to_resolution) 3600 24))
    :seconds-of-customer-impact (-> i :duration_of_customer_impact)
    :description (-> i :description)))

(comment
  (write-csv-from-maps
   "/tmp/blameless.csv"
   (->> (incidents tok "packet")
        (map
         #(select-keys* % [[:_id]
                           [:created :$date (comp str excel-time)]
                           [:type]
                           [:severity]
                           [:description]]))))

  (write-csv-from-maps
   "/tmp/blameless.csv"
   (->> (incidents tok "packet")
        (map enhance-incident))))
