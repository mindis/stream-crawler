/*
 * Copyright (c) 2015, streamdrill UG (haftungsbeschränkt)
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice, this
 *    list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR
 * ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package streamdrill.crawler

import java.io._
import java.net.{InetAddress, URLEncoder, UnknownHostException}
import java.util.zip.GZIPInputStream

import grizzled.slf4j.Logging
import org.apache.commons.codec.binary.Base64
import org.apache.http.HttpStatus._
import org.apache.http.HttpVersion
import org.apache.http.client.HttpResponseException
import org.apache.http.client.methods.{HttpGet, HttpPost, HttpRequestBase}
import org.apache.http.conn.params.ConnRoutePNames
import org.apache.http.entity.ByteArrayEntity
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.http.params.{CoreConnectionPNames, HttpProtocolParams, SyncBasicHttpParams}
import twitter4j.{RequestMethod, HttpRequest, HttpParameter}
import twitter4j.auth.{AccessToken, OAuthAuthorization}
import twitter4j.conf.ConfigurationBuilder

import scala.io.Source
import scala.xml.{Elem, NodeSeq, XML}

/**
 * Twitter Stream API Connector. Access the twitter stream and stores it in files.
 * Also works with GNIP streams.
 *
 * @author Matthias L. Jugel
 */

class Crawler(config: Elem) extends Logging {
  val apiKey = (config \ "apiKey").text
  val apiSecret = (config \ "apiSecret").text
  val oAuthConfiguration = new ConfigurationBuilder()
      .setOAuthConsumerKey(apiKey)
      .setOAuthConsumerSecret(apiSecret)
      .build

  val connection = config \ "connection"

  val gnip = try {(config \ "connection" \ "@gnip").text.toBoolean} catch {case e: Exception => false}

  val connectionTimeout = (connection \ "@connect").headOption match {
    case Some(x) => x.text.toInt
    case None => 0
  }

  val readTimeout = (connection \ "@read").headOption match {
    case Some(x) => x.text.toInt
    case None => -1
  }

  val localAddress = (connection \ "@local").headOption match {
    case Some(x) => Some(x.text)
    case None => None
  }

  val accessToken = (connection \ "auth" \ "token").text
  val accessSecret = (connection \ "auth" \ "secret").text

  val url = (connection \ "url").text
  val paramCache = (connection \ "query" \ "@cache").text.trim match {
    case "" => new File("crawler_cache.xml")
    case n: String => new File(n)
  }

  var params = getParams(connection \ "query")

  val saveDir = (config \ "target" \ "@dir").text.trim

  val datedFileWriter = RollingFileWriter(saveDir)

  val httpParams = new SyncBasicHttpParams()
  if (connectionTimeout > 0) {
    info("setting connection timeout to %dms".format(connectionTimeout))
    httpParams.setIntParameter(CoreConnectionPNames.CONNECTION_TIMEOUT, connectionTimeout)
  }
  if (readTimeout > -1) {
    if (readTimeout == 0)
      info("setting read timeout to wait infinitely")
    else
      info("setting read timeout to %dms".format(readTimeout))
    httpParams.setIntParameter(CoreConnectionPNames.SO_TIMEOUT, readTimeout)
  }
  try {
    localAddress.foreach {
      addr => httpParams.setParameter(ConnRoutePNames.LOCAL_ADDRESS, InetAddress.getByName(addr))
    }
  } catch {
    case e: UnknownHostException =>
      error("can't bin local address: %s".format(localAddress.toString))
      System.exit(-1)
  }

  HttpProtocolParams.setVersion(httpParams, HttpVersion.HTTP_1_1)
  HttpProtocolParams.setContentCharset(httpParams, "UTF-8")
  HttpProtocolParams.setUserAgent(httpParams, "stream-connector/3.0")

  val httpClient = new DefaultHttpClient(httpParams)
  var retryBackoff = 10.0
  var retryCount = 0

  var runnable: Option[Worker] = None
  var executor: Option[Thread] = None

  def restart() {
    stop()
    start()
  }

  def stop() {
    debug("stopping crawler")
    runnable.foreach(_.stop())
    runnable = None
  }

  def start() {
    if (runnable.isEmpty) {
      runnable = Some(new Worker)
      executor = Some(new Thread(runnable.get))
      executor.get.start()
    }
  }

  class Worker extends Runnable {
    var forceStop = false
    var request: Option[HttpRequestBase] = None

    def stop() {
      forceStop = true
      request.foreach(_.abort())
      executor.foreach(_.join())
    }

    def run() {
      debug("crawler running: %s".format(this))
      while (!forceStop) {
        if (params.nonEmpty) {
          info("parameters '%s'".format(params.map(p => "%s=%s".format(p._1, p._2.trim)).mkString("&")))
          if (params.contains("track")) info("rules: %d".format(params("track").split(",").size))
        }

        try {
          info("requesting resource %s".format(url))
          request = if (gnip) Some(new HttpGet(url)) else Some(new HttpPost(url))
          request.foreach {
            request =>
              if (gnip) {
                val auth = "Basic " + new String(Base64
                    .encodeBase64((accessToken + ":" + accessSecret).getBytes("UTF-8")), "UTF-8")
                request.setHeader("Authorization", auth)
                request.setHeader("Accept-encoding", "gzip")
              } else {
                request.setHeader("Content-Type", "application/x-www-form-urlencoded")
                request.setHeader("Authorization", authorizationHeader(url, params, accessToken, accessSecret))
                request.asInstanceOf[HttpPost]
                    .setEntity(new ByteArrayEntity(postParameterBody(params).getBytes("UTF-8")))
              }

              val response = httpClient.execute(request)
              response.getStatusLine.getStatusCode match {
                case SC_OK =>
                  retryBackoff = 10.0
                  retryCount = 0

                  val stream = if (gnip) {
                    new BufferedReader(new InputStreamReader(new GZIPInputStream(response.getEntity.getContent)))
                  } else {
                    new BufferedReader(new InputStreamReader(response.getEntity.getContent))
                  }

                  var line = stream.readLine
                  while (!forceStop && line != null) {
                    if (!line.startsWith("{\"limit\":") && !line.startsWith("{\"delete\":")) {
                      datedFileWriter.write(line)
                    }
                    line = stream.readLine
                  }
                  stream.close()
                case _ =>
                  error("request error: %s".format(response.getStatusLine))
                  response.getAllHeaders.foreach(h => error("%s=%s".format(h.getName, h.getValue)))
                  error(Source.fromInputStream(response.getEntity.getContent).mkString)
                  throw new HttpResponseException(response.getStatusLine.getStatusCode, response.getStatusLine.toString)
              }
          }
        } catch {
          case e: Exception if !forceStop =>
            error("streaming connection error: %s".format(e))
            try {request.foreach(_.abort())} catch {case e: Exception => /* ignore errors */}

            retryCount += 1

            val duration = retryBackoff * (1 << retryCount) match {
              case w if w > 60000 => 60000
              case w => w
            }

            error("trying again in %.2f seconds (%d retries)...".format(duration / 1000, retryCount))
            Thread.sleep(duration.toInt)
          case e: Exception =>
            error("streaming connection aborted: %s".format(e))
        }
      }
      debug("crawler stopped: %s".format(this))
    }
  }

  def getParams(defaults: NodeSeq): Map[String, String] = {
    val data = try {
      if (paramCache.exists) {
        info("taking parameters from cache: %s".format(paramCache))
        XML.load(new FileReader(paramCache))
      } else {
        defaults
      }
    } catch {
      case e: Exception =>
        error("unable to load cache: %s (%s)".format(paramCache, e.getMessage))
        defaults
    }

    (data \ "param").map(n => (n \ "@key").text -> n.text).toMap
  }

  def saveParams(xml: Elem) {
    try {
      val writer = new FileWriter(paramCache)
      XML.write(writer, xml, "UTF-8", xmlDecl = true, null)
      writer.flush()
      writer.close()
    } catch {
      case e: Exception =>
        error("unable to save cache: %s (%s)".format(paramCache, e.getMessage))
    }
  }

  // return the oAuth signature header for the url and parameters
  private def authorizationHeader(url: String, params: Map[String, String], token: String, secret: String) = {
    val httpParams = params.map(p => new HttpParameter(p._1, p._2)).toArray
    val oauth: OAuthAuthorization = new OAuthAuthorization(oAuthConfiguration)
    oauth.setOAuthAccessToken(new AccessToken(token, secret))
    val httpRequest: HttpRequest = new HttpRequest(RequestMethod.POST, url, httpParams, oauth, null)
    oauth.getAuthorizationHeader(httpRequest)
  }

  // get the parameter string concatenated and escaped
  private def postParameterBody(params: Map[String, String]) =
    params.map(p => "%s=%s".format(encode(p._1), encode(p._2))).mkString("&")

  // urlencode a string and fix the +
  private def encode(s: String) = URLEncoder.encode(s, "UTF-8").replaceAll("\\+", "%20")


}