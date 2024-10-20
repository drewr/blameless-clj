(ns blameless.core
  (:require [cheshire.core :as json]
            [clj-http.client :as http]
            [clojure.data.csv :as csv]
            [flatland.ordered.map :refer [ordered-map]]
            [java-time.api :as time]))

(def MAX_INCIDENTS 100)

(defn select-keys* [m paths]
  (into {} (map (fn [p]
                  (if (fn? (peek p))
                    (let [f (peek p)
                          all-but-the-fn (-> p reverse rest reverse)]
                      [(peek all-but-the-fn) (f (get-in m all-but-the-fn))])
                    [(peek p) (get-in m p)])))
        paths))

(defn url-base [org]
  (format "https://%s.blameless.io" org))

(defn events-url [org incident]
  (str (url-base org) (format "/api/v1/incidents/%s/events" incident)))

(defn incident-url [org]
  (str (url-base org) "/api/v1/incidents"))

(defn token-url [org]
  (str (url-base org) "/api/v2/identity/token"))

(defn get-token [org key]
  (->
   (http/post (token-url org) {:headers {:authorization key}})
   :body
   (json/decode true)
   :access_token))

(defn utc-time [epoch]
  (-> epoch
      time/instant
      (time/zoned-date-time "UTC")))

(defn local-time [epoch]
  (-> (utc-time epoch)
      (time/zoned-date-time (.getZone (time/system-clock)))))

(defn excel-time [epoch]
  (time/format "yyyy-MM-dd HH:mm:ss" (utc-time epoch)))

(defn days-ago-millis [num-days]
  (time/to-millis-from-epoch
   (time/minus
    (time/zoned-date-time) (time/days num-days))))

(def incidents*
  (memoize
   (fn
     ([org tok query-params]
      (incidents* org tok query-params nil))
     ([org tok query-params cache-key]
      (let [resp (-> (http/get
                      (incident-url org)
                      {:headers {:authorization (str "Bearer " tok)}
                       :query-params query-params})
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
             (merge query-params
                    {:total-count (-> resp :pagination :count)
                     :this-count (count is)}))
        (lazy-seq
         (when (seq is)
           (cons (first is)
                 (concat
                  (rest is)
                  (incidents*
                   org tok
                   (merge query-params
                          {:offset (new-offset resp)
                           :limit (new-limit resp)})
                   :NOT_IMPLEMENTED))))))))))

(def incidents
  (fn [org tok]
    (incidents*
     org tok {:offset 0
              :limit MAX_INCIDENTS
              :order_by "created"})))

(defn incidents-since [org tok epoch-millis]
  (incidents*
   org tok
   {:offset 0
    :limit MAX_INCIDENTS
    :order_by "created"
    :created_from (str
                   (int (/ epoch-millis 1000)))}))

(defn set-incident-status! [org tok id status]
  (-> (http/put
       (format "%s/%s" (incident-url org) id)
       {:headers {:authorization (str "Bearer " tok)
                  :content-type "application/json"}
        :body (json/encode {:status status})})
      :body
      (json/decode true)
      (select-keys* [[:ok]
                     [:incident :_id]
                     [:incident :type]
                     [:incident :severity]
                     [:incident :status]
                     [:incident :description]
                     [:events count]])))

(defn events [org tok incident]
  (let [resp (-> (http/get
                  (events-url org incident)
                  {:headers {:authorization (str "Bearer " tok)}
                   :query-params {}})
                 :body (json/decode true))]
    resp))

(defn incident-scorecard [org tok incident]
  (->> (events org tok incident)
       :events
       (map #(select-keys* % [[:source :profile :email]]))
       (reduce #(update-in %1 [(:email %2)] (fnil inc 0)) {})))

(def severity-count
  (comp))

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

(defn creator-email [incident]
  (let [f #(= (:creator incident) (:_id %))]
    (-> (first (filter f (:team incident)))
        :profile
        :email)))

(defn enhance-incident [i]
  (assoc (ordered-map)
    :id (-> i :_id)
    :created-utc (-> i :created :$date excel-time str)
    :type (-> i :type)
    :severity (-> i :severity)
    :creator (creator-email i)
    :seconds-to-resolve (-> i :time_to_resolution)
    :hours-to-resolve (int (/ (-> i :time_to_resolution) 3600))
    :days-to-resolve (int (/ (-> i :time_to_resolution) 3600 24))
    :hours-of-customer-impact (float
                               (/ (-> i :duration_of_customer_impact) 3600))
    :seconds-of-customer-impact (-> i :duration_of_customer_impact)
    :description (-> i :description)))

(defn just-severity [sev]
  (when (string? sev)
    (re-find #"[^:]+" sev)))

(defn just-ymd [excel-time]
  (re-find #"[^\s]+" excel-time))

(comment
  (write-csv-from-maps
   "/tmp/blameless.csv"
   (->> (incidents "packet" tok)
        (map
         #(select-keys* % [[:_id]
                           [:created :$date (comp str excel-time)]
                           [:type]
                           [:severity]
                           [:description]]))))

  (write-csv-from-maps
   "/tmp/blameless.csv"
   (->> (incidents "packet" tok)
        (map enhance-incident))))
