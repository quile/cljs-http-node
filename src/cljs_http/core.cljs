(ns cljs-http.core
  (:require-macros [cljs.core.async.macros :refer [go]])
  (:require [cljs-http.util :as util]
            [cljs.nodejs :as node]
            [cljs.core.async :refer [chan >! <! put!] :as async]))

(def pending-requests (atom {}))

(defn abort!
  "Attempt to close the given channel and abort the pending HTTP request
  with which it is associated."
  [channel]
  (when-let [xhr (@pending-requests channel)]
    (swap! pending-requests dissoc channel)
    (async/close! channel)
    (.abort xhr)))

(comment
  (defn request
    "Execute the HTTP request corresponding to the given Ring request
    map and return a core.async channel."
    [{:keys [request-method headers body with-credentials?] :as request}]
    (let [channel (async/chan)
          request-url (util/build-url request)
          method (name (or request-method :get))
          timeout (or (:timeout request) 0)
          headers (util/build-headers headers)
          send-credentials (if (nil? with-credentials?)
                             true
                             with-credentials?)
          xhr (doto (XhrIo.)
                (.setTimeoutInterval timeout)
                (.setWithCredentials send-credentials))]
      (swap! pending-requests assoc channel xhr)
      (.listen xhr EventType.COMPLETE
               #(let [target (.-target %1)]
                  (->> {:status (.getStatus target)
                        :success (.isSuccess target)
                        :body (.getResponseText target)
                        :headers (util/parse-headers (.getAllResponseHeaders target))
                        :trace-redirects [request-url (.getLastUri target)]}
                       (async/put! channel))
                  (swap! pending-requests dissoc channel)
                  (async/close! channel)))
      (.send xhr request-url method body headers)
      channel)))

(def http (node/require "http"))
(def https (node/require "https"))

(defn ->node-req
  [req]
  (clj->js {:method (or (:request-method req) :get)
            :port (or (:server-port req) 80)
            :hostname (:server-name req)
            :path (:uri req)
            :headers (:headers req)}))

(defn clean-response
  [res]
  (assoc res :body (->> res :body clj->js (.concat js/Buffer) js->clj)
             :status (-> res :status first (or 200))
             :headers (->> res :headers (apply merge))))

(defn request
  "Execute the HTTP request using the node.js primitives"
  [{:keys [request-method headers body with-credentials? protocol] :as request}]
  (let [request-url (util/build-url request)
        method (or request-method :get)
        timeout (or (:timeout request) 0)
        content-length (or (when body (.-length body)) 0)
        _ (prn content-length)
        headers (util/build-headers (assoc headers "content-length" content-length))
        js-request (->node-req (assoc request :headers headers))
        scheme (or protocol
                   (if (= (:scheme request) :https)
                     https
                     http))
        ;; This needs a stream abstraction!
        chunks-ch (chan)
        response-ch (chan)
        client (.request scheme js-request
                         (fn [js-res]
                           (put! chunks-ch {:headers (-> js-res
                                                         (aget "headers")
                                                         (js->clj :keywordize-keys true))})
                           (put! chunks-ch {:status (.-statusCode js-res)})
                           (doto js-res
                             (.on "data" (fn [stuff] (put! chunks-ch {:body stuff})))
                             (.on "end" (fn [] (async/close! chunks-ch))))))]
    (go (loop [response {:status [] :body [] :headers []}]
          (let [stuff (<! chunks-ch)]
            (if (nil? stuff)
              (do (>! response-ch (clean-response response)) (async/close! response-ch))
              (recur (merge-with conj response stuff))))))

    (when body
      (do (prn body) (.write client body)))

    (doto client
      (.on "error" (fn [error]
                     (do
                       (.log js/console (str "Error in request: " error))
                       (put! chunks-ch {:status -1 :error error})
                       (async/close! chunks-ch))))
      (.end))
    response-ch))
