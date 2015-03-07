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

import javax.servlet.http.{HttpServletRequest, HttpServletResponse}

import grizzled.slf4j.Logging
import org.eclipse.jetty.server.Request
import org.eclipse.jetty.server.handler.AbstractHandler

import scala.xml.XML

/**
 * Simple web handler for remote setting of the request parameters
 *
 * @author Matthias L. Jugel
 */

class ControlHandler(crawler: Crawler, password: String) extends AbstractHandler with Logging {
  def handle(target: String, baseRequest: Request, request: HttpServletRequest, response: HttpServletResponse) {
    val auth = request.getHeader("Authorization")
    if(auth != null && auth.equals(password)) {
      response.setContentType("text/plain;charset=utf-8")
      response.setStatus(HttpServletResponse.SC_OK)

      try {
        val data = XML.load(request.getInputStream)
        val params = (data \ "param").map(n => (n \ "@key").text -> n.text).toMap

        val curParams = crawler.params.toSeq.map(p => p._1 + "=" + p._2)
        val newParams = params.toSeq.map(p => p._1 + "=" + p._2)

        if (newParams.diff(curParams).nonEmpty) {
          crawler.params = params
          crawler.saveParams(data)
          crawler.restart()
          response.getWriter.println("OK")
        } else {
          response.getWriter.println("OK (unchanged)")
        }

      } catch {
        case e: Exception =>
          response.getWriter.println("ERROR (%s)".format(e.getMessage))
      }
    } else {
      response.setStatus(HttpServletResponse.SC_UNAUTHORIZED)
    }
    baseRequest.setHandled(true)
  }
}
