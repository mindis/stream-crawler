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

import java.io.FileInputStream

import grizzled.slf4j.Logging
import org.apache.commons.codec.binary.Base64
import org.eclipse.jetty.server.Server

import scala.xml.XML

/**
 * Twitter Stream API collector with remote configurable request parameters.
 *
 * @author Matthias L. Jugel
 */

object Main extends App with Logging {
  info("Twitter Stream API Collector")
  info("(c) 2015 streamdrill. All rights reserved.")

  if (args.length != 1) {
    println("usage: java -jar crawler.jar config.xml")
    System.exit(1)
  }
  val config = XML.load(new FileInputStream(args(0)))

  val user = (config \ "remote" \ "@user").text.trim
  val pass = (config \ "remote" \ "@password").text.trim
  if (user.length == 0 || pass.length == 0) {
    error("!! Remote user and password must be set.")
    System.exit(-1)
  }

  val crawler = new Crawler(config)
  crawler.start()

  val port = (config \ "remote" \ "@port").text match {
    case "" => 7799
    case n: String => n.toInt
  }


  val password = "Basic " + new String(Base64.encodeBase64((user + ":" + pass).getBytes("UTF-8")), "UTF-8")

  val jettyServer = new Server(port)
  jettyServer.setHandler(new ControlHandler(crawler, password))
  jettyServer.start()
  jettyServer.join()
}


