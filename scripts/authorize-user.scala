/**
 * Convenience method to receive a user token and token secret from the command line.
 *
 * @author Matthias L. Jugel
 */
import twitter4j.{TwitterException, TwitterFactory}

import scala.io.StdIn

val consumerKey = "YOUR CONSUMER KEY HERE"
val ConsumerSecret = "YOUR CONSUMER SECRETE HERE"

var twitter = new TwitterFactory().getInstance
twitter.setOAuthConsumer(consumerKey, ConsumerSecret)
var requestToken = twitter.getOAuthRequestToken
println("Open the following URL and grant access to your account:")
println(requestToken.getAuthorizationURL)
print("Enter the PIN(if available) or just hit enter. [PIN]:")
try {
  val token = StdIn.readLine() match {
    case null =>
      println("STOP")
      None
    case pin: String if pin.trim.length > 0 =>
      Some(twitter.getOAuthAccessToken(requestToken, pin.trim))
    case pin: String if pin.trim.length == 0 =>
      Some(twitter.getOAuthAccessToken)
  }

  token match {
    case Some(t) =>
      println()
      println("== %s (%d) ==".format(t.getScreenName, t.getUserId))
      println("token : %s".format(t.getToken))
      println("secret: %s".format(t.getTokenSecret))

    case None =>
      println("STOP")
      System.exit(0)
  }
} catch {
  case te: TwitterException if te.getStatusCode == 401 =>
    println("Unable to get the access token: %s".format(te.getMessage))
  case e: Throwable =>
    e.printStackTrace()
}
System.exit(0)