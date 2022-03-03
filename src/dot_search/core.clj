(ns dot-search.core
  (:require [http.async.client :as http]
            [clj-http.client :as client]))

(def sms_pass "SMS_C_Pass_2021Aug.txt")

(def sms_pass_sep "SMS_C_Pass_2021Sep.txt")

(def basic_violations "test2.txt")

(def basic_violations2 "basic_data2.csv")

;; (def sep_basic "sep-basic1.csv")

(def sep_basic "sep_basic.csv")

(def sep_violation "violation_sep.csv")

(def fmcsa_2021 "test1.txt")

(def dot_list "dot_list.csv")

(def sms_pass_ab "SMS_AB.txt")

(slurp sms_pass_sep)


(def path "https://mobile.fmcsa.dot.gov/qc/services/carriers/")

(def web-key "?webKey=e0dc87b31ac11f50e53512f72137e57a438ed74b")

(def web-key-basic "/basics?webKey=e0dc87b31ac11f50e53512f72137e57a438ed74b")

(def dot-map ["crashTotal" "dbaName" "dotNumber" "driverInsp" "ein" "phyCity" "phyCountry" "phyState" "phyStreet" "phyZipcode" "safetyReviewDate"])

(def dot-basic-map ["basicsId" "dotNumber" "measureValue" "seriousViolationFromInvestigationPast12MonthIndicator" "totalInspectionWithViolation" "totalViolation"])




(defn for-str  [string]
  (loop [cnt-str 0]
    (when (< cnt-str (count string))
      (loop [cnt-dot 0]
        (when (< cnt-dot (count dot-basic-map))
          (if (clojure.string/includes? (get string cnt-str) (get dot-basic-map cnt-dot))
            (spit "sep_basic.csv" (str (last (clojure.string/split (get string cnt-str) #":")) (if (<= (- (count dot-basic-map) 1) cnt-dot) "\n" ",")) :append true))
          (recur (inc cnt-dot))))
      (recur (inc cnt-str)))))

(defn parse-dot [string]
  (map #(clojure.string/split % #",")
       (clojure.string/split string #"\n")))

(defn print-to-csv [string]
  (->> string
       parse-dot
       last
       for-str))

;;good version of web-calls-to-csv-basic
 (loop [i 7000]
   (when (> i 1000)
     (client/get (str path i web-key-basic)
                 {:async? true}
             ;; response callback
                 (fn [response]  (if (> 66 (count (get response :body)))
                                   nil
                                   (print-to-csv (get response :body)))
                   (Thread/sleep 15))
             ;; raise callback
                 (fn [exception] (println "exception message is: " (.getMessage exception))))
     (Thread/sleep 10)
     (recur (- i 1))))






;; (get dot-map 0)
;; (for [i (range (count dot-map))]
;;     (spit "data.csv" (str (second (clojure.string/split (get (last dot-issue) (dot-map i)) #":")) (if (<= (- (count dot-map) 1) i) "\n" ",")) :append true)
;; )

;; (for [i (range (count dot-map))]
;;   (do (println (get dot-map i))
;;     (println (get (last dot-issue) (dot-map i)))
;;     ))



;; (for [i (range (count dot-map))]
;;     (spit "data.csv" (str (second (clojure.string/split (get (last dot-issue) (get dot-map i)) #":")) (if (<= (- (count dot-map) 1) i) "\n" ",")) :append true)
;; )

;; (spit "data.csv" (str (second (clojure.string/split (get (last dot-issue) (dot-map 1)) #":")) ",") :append true)




(defn analysis
  [text]
  (str "Character count: " (count text)))

(defn analyze-file
  [filename]
  (analysis (slurp filename)))

(analyze-file sms_pass_sep)

(def dot-violation-keys [:DOT_NULL :BASIC_ID :BASIC_ID2 :DOT_NUMBER :MEASURE_VALUE :SERIOUS_VIOLATION_PAST_YEAR :TOTAL_INSPECTION_WITH_VIOLATIONS :TOTAL_VIOLATIONS])

(def dot-data-keys [:CRASH_TOTAL :DBA_NAME :DOT_NUMBER :DRIVER_INSP_TOTAL :EIN  :LEGAL_NAME :PHY_CITY :PHY_COUNTRY :PHY_STATE :PHY_STREET :PHY_ZIP :SAFETY_REVIEW_DATE :DRIVER_TOTAL])

(def dot-violation-keys-ab [:DOT_NUMBER :INSP_TOTAL :DRIVER_INSP_TOTAL :DRIVER_OOS_INSP_TOTAL :VEHICLE_INSP_TOTAL :VEHICLE_OOS_INSP_TOTAL :UNSAFE_DRIV_INSP_W_VIOL :UNSAFE_DRIV_MEASURE :UNSAFE_DRIV_AC :HOS_DRIV_INSP_W_VIOL :HOS_DRIV_MEASURE :HOS_DRIV_AC :DRIV_FIT_INSP_W_VIOL :DRIV_FIT_MEASURE :DRIV_FIT_AC :CONTR_SUBST_INSP_W_VIOL :CONTR_SUBST_MEASURE :CONTR_SUBST_AC :VEH_MAINT_INSP_W_VIOL :VEH_MAINT_MEASURE :VEH_MAINT_AC])

(def dot_sms_c_pass_sep [:DOT_NUMBER :INSP_TOTAL :DRIVER_INSP_TOTAL :DRIVER_OOS_INSP_TOTAL :VEHICLE_INSP_TOTAL :VEHICLE_OOS_INSP_TOTAL :UNSAFE_DRIV_INSP_W_VIOL :UNSAFE_DRIV_MEASURE :UNSAFE_DRIV_PCT :UNSAFE_DRIV_RD_ALERT :UNSAFE_DRIV_AC :UNSAFE_DRIV_BASIC_ALERT :HOS_DRIV_INSP_W_VIOL :HOS_DRIV_MEASURE :HOS_DRIV_PCT :HOS_DRIV_RD_ALERT :HOS_DRIV_AC :HOS_DRIV_BASIC_ALERT :DRIV_FIT_INSP_W_VIOL :DRIV_FIT_MEASURE :DRIV_FIT_PCT :DRIV_FIT_RD_ALERT :DRIV_FIT_AC :DRIV_FIT_BASIC_ALERT :CONTR_SUBST_INSP_W_VIOL :CONTR_SUBST_MEASURE :CONTR_SUBST_PCT :CONTR_SUBST_RD_ALERT :CONTR_SUBST_AC :CONTR_SUBST_BASIC_ALERT :VEH_MAINT_INSP_W_VIOL :VEH_MAINT_MEASURE :VEH_MAINT_PCT :VEH_MAINT_RD_ALERT :VEH_MAINT_AC :VEH_MAINT_BASIC_ALERT])

(def dot-info-keys [:DOT_NUMBER :LEGAL_NAME :DBA_NAME :CARRIER_OPERATION :HM_FLAG :PC_FLAG :PHY_STREET :PHY_CITY :PHY_STATE :PHY_ZIP :PHY_COUNTRY :MAILING_STREET :MAILING_CITY :MAILING_STATE :MAILING_ZIP :MAILING_COUNTRY :TELEPHONE :FAX :EMAIL_ADDRESS :MCS150_DATE :MCS150_MILEAGE :MCS150_MILEAGE_YEAR :ADD_DATE :OIC_STATE :NBR_POWER_UNIT :DRIVER_TOTAL])

(defn str->int
  [str]
  (if (or (= "null" str) (= "N" str) (= "Y" str) (clojure.string/includes? str "."))
    (Integer. "0")
    (Integer. str)))

(defn str->dbl
  [str]
  (if (or (= "null" str) (= "N" str) (= "Y" str))
    (Double/parseDouble "0.0")
    (Double/parseDouble str)))


(def conversions-basic-violation {:DOT_NULL identity
                                  :BASIC_ID str->int
                                  :BASIC_ID2 str->int
                                  :DOT_NUMBER str->int
                                  :MEASURE_VALUE str->dbl
                                  :SERIOUS_VIOLATION_PAST_YEAR identity
                                  :TOTAL_INSPECTION_WITH_VIOLATIONS str->int
                                  :TOTAL_VIOLATIONS str->int})

(def conversions-basic-ab {:DOT_NUMBER str->int
                           :INSP_TOTAL str->int
                           :DRIVER_INSP_TOTAL str->int
                           :DRIVER_OOS_INSP_TOTAL str->int
                           :VEHICLE_INSP_TOTAL str->int
                           :VEHICLE_OOS_INSP_TOTAL str->int
                           :UNSAFE_DRIV_INSP_W_VIOL str->int
                           :UNSAFE_DRIV_MEASURE str->dbl
                           :UNSAFE_DRIV_AC identity
                           :HOS_DRIV_INSP_W_VIOL str->int
                           :HOS_DRIV_MEASURE str->dbl
                           :HOS_DRIV_AC identity
                           :DRIV_FIT_INSP_W_VIOL str->dbl
                           :DRIV_FIT_MEASURE str->dbl
                           :DRIV_FIT_AC identity
                           :CONTR_SUBST_INSP_W_VIOL str->dbl
                           :CONTR_SUBST_MEASURE str->dbl
                           :CONTR_SUBST_AC identity
                           :VEH_MAINT_INSP_W_VIOL str->int
                           :VEH_MAINT_MEASURE str->dbl
                           :VEH_MAINT_AC identity})


(def conversions-basic {:DOT_NUMBER str->int
                        :INSP_TOTAL str->int
                        :DRIVER_INSP_TOTAL str->int
                        :DRIVER_OOS_INSP_TOTAL str->int
                        :VEHICLE_INSP_TOTAL str->int
                        :VEHICLE_OOS_INSP_TOTAL str->int
                        :UNSAFE_DRIV_INSP_W_VIOL str->int
                        :UNSAFE_DRIV_MEASURE str->dbl
                        :UNSAFE_DRIV_PCT identity
                        :UNSAFE_DRIV_RD_ALER identity
                        :UNSAFE_DRIV_AC identity
                        :UNSAFE_DRIV_BASIC_ALERT identity
                        :HOS_DRIV_INSP_W_VIOL str->int
                        :HOS_DRIV_MEASURE str->dbl
                        :HOS_DRIV_PCT identity
                        :HOS_DRIV_RD_ALERT identity
                        :HOS_DRIV_AC identity
                        :HOS_DRIV_BASIC_ALERT identity
                        :DRIV_FIT_INSP_W_VIOL str->dbl
                        :DRIV_FIT_MEASURE str->dbl
                        :DRIV_FIT_PCT identity
                        :DRIV_FIT_RD_ALERT identity
                        :DRIV_FIT_AC identity
                        :DRIV_FIT_BASIC_ALERT identity
                        :CONTR_SUBST_INSP_W_VIOL str->dbl
                        :CONTR_SUBST_MEASURE str->dbl
                        :CONTR_SUBST_PCT identity
                        :CONTR_SUBST_RD_ALERT identity
                        :CONTR_SUBST_AC identity
                        :CONTR_SUBST_BASIC_ALERT identity
                        :VEH_MAINT_INSP_W_VIOL str->int
                        :VEH_MAINT_MEASUR str->dbl
                        :VEH_MAINT_PCT identity
                        :VEH_MAINT_RD_ALERT identity
                        :VEH_MAINT_AC identity
                        :VEH_MAINT_BASIC_ALERT identity})

(def conversions-detail {:DOT_NUMBER str->int
                         :LEGAL_NAME identity
                         :DBA_NAME identity
                         :CARRIER_OPERATION identity
                         :HM_FLAG identity
                         :PC_FLAG identity
                         :PHY_STREET identity
                         :PHY_CITY identity
                         :PHY_STATE identity
                         :PHY_ZIP identity
                         :PHY_COUNTRY identity
                         :MAILING_STREET identity
                         :MAILING_CITY identity
                         :MAILING_STATE identity
                         :MAILING_ZIP identity
                         :MAILING_COUNTRY identity
                         :TELEPHONE identity
                         :FAX identity
                         :EMAIL_ADDRESS identity
                         :MCS150_DATE identity
                         :MCS150_MILEAGE identity
                         :MCS150_MILEAGE_YEAR identity
                         :ADD_DATE identity
                         :OIC_STATE identity
                         :NBR_POWER_UNIT identity
                         :DRIVER_TOTAL identity})

(defn convert-detail
  [dot-keys value]
  ((get conversions-detail dot-keys) value))

(defn convert-basic
  [dot-keys value]
  ((get conversions-basic dot-keys) value))

(defn convert-basic-violations
  [dot-keys value]
  ((get conversions-basic-violation dot-keys) value))

(defn convert-basic-ab
  [dot-keys value]
  ((get conversions-basic-ab dot-keys) value))


(defn parse
  "Convert a CSV into rows of columns"
  [string]
  (map #(clojure.string/split % #",")
       (clojure.string/split string #"\n")))

(defn mapify-basic
  "Return a seq of maps like {:name \"Edward Cullen\" :glitter-index 10}"
  [rows dot-keys convert]
  (map (fn [unmapped-row]
         (if (= 36 (count unmapped-row))
           (reduce (fn [row-map [dot-key value]]
                     (assoc row-map dot-key (convert dot-key value)))
                   {}
                   (map vector dot-keys unmapped-row))))
       rows))

(defn mapify
  "Return a seq of maps like {:name \"Edward Cullen\" :glitter-index 10}"
  [rows dot-keys convert]
  (map (fn [unmapped-row]
         (if (= 8 (count unmapped-row))
           (reduce (fn [row-map [dot-key value]]
                     (assoc row-map dot-key (convert dot-key value)))
                   {}
                   (map vector dot-keys unmapped-row))))
       rows))


(defn violation-filter
  [dot-key minimum-violation records]
  (filter #(>= (dot-key %) minimum-violation) records))

(defn violation-filter-drugs
  [records]
  (filter #(= (:BASIC_ID %) 14) records))

(defn violation-filter-drugs-measures
  [records]
  (filter #(>= (:MEASURE_VALUE %) 0.1) records))

(defn drug-violations 
  [rec]
  (->> rec
       violation-filter-drugs
       violation-filter-drugs-measures))

(defn seach-filter
  [dot-key value records]
  (filter #(= (dot-key %) value) records))

(defn check-past-year 
  [dot-key value records]
  (filter #(= (dot-key %) value) records))

;; (count (mapify (parse (slurp sms_pass_sep)) dot_sms_c_pass_sep conversions-basic)
;; )
       
;; (p/try
;;  (p/let [resp (fetch/get
;;                "https://mobile.fmcsa.dot.gov/qc/services/carriers/%201022698?webKey=e0dc87b31ac11f50e53512f72137e57a438ed74b"
;;                {:accept :json
;;                 :content-type :json})]
;;         (prn (:body resp)))
;;  (p/catch :default e
;;           ;; log your exception here
;;           (prn :error e)))

;; (violation-filter :CONTR_SUBST_INSP_W_VIOL 0.3 (mapify (parse (slurp sms_pass)) dot-info-keys convert-detail))


;;These three calls call our drug violations
 (count (mapify (parse (slurp sep_violation)) dot-violation-keys convert-basic-violations)
)
 
  (count (mapify (parse (slurp sep_basic)) dot-violation-keys convert-basic-violations)
)
 
  (count (mapify (parse (slurp dot_list)) dot-violation-keys convert-basic-violations)
)

 (count (remove nil? (mapify (parse (slurp sep_basic)) dot-violation-keys convert-basic-violations)
))

(count (drug-violations (remove nil? (mapify (parse (slurp basic_violations2)) dot-violation-keys convert-basic-violations)
)))
 
 (count (check-past-year   :SERIOUS_VIOLATION_PAST_YEAR  "Y" (drug-violations (remove nil? (mapify (parse (slurp basic_violations2)) dot-violation-keys convert-basic-violations)
))))

 ;;we need to get each value out of the map, might need a map of keys to iterate through
(defn print-past-year-viol
  [recs]
  (for [i recs]
    (loop [cnt-map 0]
      (when (< cnt-map (count (doall i)))
        (spit "violation_sep_y_investigation.csv"  (str (get (doall i) (get dot-violation-keys cnt-map)) (if (<= (- (count (doall i)) 1) cnt-map) "\n" ",")) :append true)
        ;;(println (get (doall i) (get dot-violation-keys cnt-map)))
        (recur (inc cnt-map))))))
 
(print-past-year-viol (check-past-year   :SERIOUS_VIOLATION_PAST_YEAR  "Y" (drug-violations (remove nil? (mapify (parse (slurp basic_violations2)) dot-violation-keys convert-basic-violations)
))))


;; (defn print-past-year-viol
;;   [records]
;;   (loop [i 0]
;;     (let [recs (doall records)]
;;     (when (< i 10)
;;       (println (take 10 recs))
;;     (recur (inc i))))))

;; (dot-violation-keys 1)


;; (defn for-str  [string]
;;   (loop [cnt-str 0]
;;     (when (< cnt-str (count string))
;;       (loop [cnt-dot 0]
;;         (when (< cnt-dot (count dot-basic-map))
;;           (if (clojure.string/includes? (get string cnt-str) (get dot-basic-map cnt-dot))
;;             (spit "data.csv" (str (last (clojure.string/split (get string cnt-str) #":")) (if (<= (- (count dot-basic-map) 1) cnt-dot) "\n" ",")) :append true))
;;           (recur (inc cnt-dot))))
;;       (recur (inc cnt-str)))))


;;  (print-past-year-viol (check-past-year   :SERIOUS_VIOLATION_PAST_YEAR  "Y" (drug-violations (remove nil? (mapify (parse (slurp basic_violations2)) dot-violation-keys convert-basic-violations)
;; ))))




;; (mapify (parse (slurp fmcsa_2021)) dot-info-keys convert-detail)

;; (count (mapify (parse (slurp fmcsa_2021)) dot-info-keys convert-detail)
;; )

;; (mapify (parse (slurp sms_pass_ab)) dot-violation-keys-ab convert-basic-ab)
;; (count (mapify (parse (slurp sms_pass_ab)) dot-violation-keys-ab convert-basic-ab)
;; )

;; (count (violation-filter :CONTR_SUBST_MEASURE 2.1 (mapify (parse (slurp sms_pass_ab)) dot-violation-keys-ab convert-basic-ab)
;; ))

;; (violation-filter :CONTR_SUBST_MEASURE 2.1 (mapify (parse (slurp sms_pass_ab)) dot-violation-keys-ab convert-basic-ab)
;; )

;; (count (violation-filter :CONTR_SUBST_INSP_W_VIOL 0.1 (mapify (parse (slurp sms_pass_ab)) dot-violation-keys-ab convert-basic-ab)
;; ))

;; ;; (violation-filter :VEH_MAINT_INSP_W_VIOL 0.8 (mapify (parse (slurp sms_pass)) dot-violation-keys))

;; (mapify (parse (slurp sms_pass_ab)) dot-violation-keys-ab convert-basic-ab)
;; (count (mapify (parse (slurp sms_pass_ab)) dot-violation-keys-ab convert-basic-ab)
;; )

;;  (count (mapify (parse (slurp sms_pass)) dot-violation-keys convert-basic))

;; (parse (slurp sms_pass))

;; (count (violation-filter :VEH_MAINT_INSP_W_VIOL 0.8 (mapify (parse (slurp sms_pass))))
;; )
