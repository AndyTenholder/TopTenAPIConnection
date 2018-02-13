using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tweetinvi;
using System.Data.SqlClient;   // System.Data.dll 
using System.Net;
using System.Security.Authentication;
using Tweetinvi.Core.Exceptions;
using System.Threading;
//using System.Data;           // For:  SqlDbType , ParameterDirection


namespace TopTenAPIConnection
{
    class Program
    {
        static void Main(string[] args)
        {
            
            SqlConnectionStringBuilder cb = new SqlConnectionStringBuilder();
            cb.DataSource = "****************************";
            cb.UserID = "****************************@****************************-server";
            cb.Password = "****************************";
            cb.InitialCatalog = "****************************-db";
            // Set up your credentials (https://apps.twitter.com)
            // Applies credentials for the current thread.If used for the first time, set up the ApplicationCredentials
            Auth.SetUserCredentials("****************************", "****************************", "****************************-****************************", "****************************");
            var user = User.GetAuthenticatedUser();
            bool twitterCreds1InUse = true;

            // Enable Automatic RateLimit handling
            RateLimit.RateLimitTrackerMode = RateLimitTrackerMode.TrackAndAwait;

            var stream = Stream.CreateSampleStream();

            stream.StreamStopped += (sender, arguems) =>
            {
                
                try
                {
                    var exception = (ITwitterException)arguems.Exception; //<-- here it fails the cast
                }
                catch (Exception ex)
                {
                    Thread.Sleep(2000);
                    stream.StartStream();
                }
                var exceptionThatCausedTheStreamToStop = arguems.Exception;
                Console.WriteLine("------------ Error {0} --------------------", exceptionThatCausedTheStreamToStop);
                var twitterDisconnectMessage = arguems.DisconnectMessage;
                Console.WriteLine("------------ Message {0} --------------------", twitterDisconnectMessage);
            };

            TweetinviEvents.QueryBeforeExecute += (sender, arguements) =>
            {
                var queryRateLimits = RateLimit.GetQueryRateLimit(arguements.QueryURL);

                // Some methods are not RateLimited. Invoking such a method will result in the queryRateLimits to be null
                if (queryRateLimits != null)
                {
                    if (queryRateLimits.Remaining > 0)
                    {
                        // We have enough resource to execute the query
                        return;
                    }

                    if (twitterCreds1InUse)
                    {
                        // Strategy #2 : Use different credentials
                        Auth.SetUserCredentials("****************************", "****************************", "****************************-****************************", "****************************");
                        user = User.GetAuthenticatedUser();
                        twitterCreds1InUse = !twitterCreds1InUse;
                    }
                    else
                    {
                        Auth.SetUserCredentials("****************************", "****************************", "****************************-****************************", "****************************");
                        user = User.GetAuthenticatedUser();
                        twitterCreds1InUse = !twitterCreds1InUse;
                    }
                    

                }
            };


            stream.TweetReceived += (sender, recievedTweet) =>
            {
                if (recievedTweet.Tweet.Hashtags.Count() > 0)
                {
                    Int32 unixTimestamp = (Int32)(DateTime.UtcNow.Subtract(new DateTime(1970, 1, 1))).TotalSeconds;
                    string tweetLanguage = recievedTweet.Tweet.Language.ToString();

                    // if language is not in DB add it
                    if (GetLanguage(recievedTweet.Tweet.Language.ToString()) == "")
                    {
                        using (var connection = new SqlConnection(cb.ConnectionString))
                        {
                            connection.Open();
                            StringBuilder sb = new StringBuilder();
                            sb.Append("INSERT INTO Languages (Name) ");
                            sb.Append(String.Format("VALUES ('{0}');", tweetLanguage));
                            String sql = sb.ToString();

                            using (var command = new SqlCommand(sql, connection))
                            {
                                Console.WriteLine("Added Language {0} to database.", tweetLanguage);
                                command.ExecuteNonQuery();
                            }
                            connection.Close();
                        }
                    }

                    // Create new tweet object and add to db

                    using (var connection = new SqlConnection(cb.ConnectionString))
                    {
                        connection.Open();
                        StringBuilder sb = new StringBuilder();
                        sb.Append("INSERT INTO Tweets (UnixTimeStamp, LanguageID, TweetIdString) ");
                        sb.Append(String.Format("VALUES ({0} , {1} , '{2}');", unixTimestamp, GetLanguageID(tweetLanguage), recievedTweet.Tweet.IdStr));
                        String sql = sb.ToString();

                        using (var command = new SqlCommand(sql, connection))
                        {
                            int rowsAffected = command.ExecuteNonQuery();
                        }
                        connection.Close();
                    }

                    List<string> hashtagList = new List<string>();

                    // if hashtag is not in DB add it
                    foreach (var hashtag in recievedTweet.Tweet.Hashtags)
                    {
                        // Convert hashtag to uppercase string
                        var upperHashtag = hashtag.ToString().ToUpper();

                        if (GetHashtag(upperHashtag) != "")
                        {

                            if (!hashtagList.Contains(upperHashtag))
                            {
                                hashtagList.Add(upperHashtag);
                            }
                        }
                        else
                        {
                            using (var connection = new SqlConnection(cb.ConnectionString))
                            {
                                connection.Open();
                                StringBuilder sb = new StringBuilder();
                                sb.Append("INSERT INTO Hashtags (Name) ");
                                sb.Append(String.Format("VALUES (N'{0}');", upperHashtag));
                                String sql = sb.ToString();

                                using (var command = new SqlCommand(sql, connection))
                                {
                                    command.ExecuteNonQuery();
                                    Console.WriteLine("Added Hashtag {0} to database.", upperHashtag);
                                }
                                connection.Close();
                            }
                            if (!hashtagList.Contains(upperHashtag))
                            {
                                hashtagList.Add(upperHashtag);
                            }
                        }

                    }

                    // Create TweetHashtag object for each hashtag
                    foreach (var hashtag in hashtagList)
                    {
                        if (GetTweetID(recievedTweet.Tweet.IdStr).Length > 7)
                        {
                            break;
                        }
                        using (var connection = new SqlConnection(cb.ConnectionString))
                        {
                            connection.Open();
                            StringBuilder sb = new StringBuilder();
                            sb.Append("INSERT INTO TweetHashtags (HashtagID, TweetID, UnixTimeStamp) ");
                            sb.Append(String.Format("VALUES ({0} , '{1}', {2});", GetHashtagID(hashtag.ToString().ToUpper()), GetTweetID(recievedTweet.Tweet.IdStr), unixTimestamp));
                            String sql = sb.ToString();

                            using (var command = new SqlCommand(sql, connection))
                            {
                                command.ExecuteNonQuery();
                            }
                            connection.Close();
                        }
                    }
                }
            };

            stream.StartStream();

            Console.ReadKey();

            // Gets ID of Tweet
            string GetTweetID(string tweetIdString)
            {
                StringBuilder result = new StringBuilder();

                using (SqlConnection connection = new SqlConnection(cb.ConnectionString))
                {
                    connection.Open();

                    StringBuilder sb = new StringBuilder();
                    sb.Append("SELECT ID ");
                    sb.Append("FROM Tweets ");
                    sb.Append(String.Format("WHERE TweetIdString={0};", tweetIdString));
                    String sql = sb.ToString();

                    SqlCommand command = new SqlCommand(sql, connection);
                    SqlDataReader reader = command.ExecuteReader();
                    if (reader.HasRows)
                    {
                        while (reader.Read())
                        {
                            result.Append(reader.GetInt32(0));
                        }
                        reader.Close();
                    }
                    connection.Close();
                }
                return result.ToString();
            }

            // Gets ID of Hashtag
            int GetHashtagID(string hashtag)
            {
                StringBuilder result = new StringBuilder();

                using (SqlConnection connection = new SqlConnection(cb.ConnectionString))
                {
                    connection.Open();

                    StringBuilder sb = new StringBuilder();
                    sb.Append("SELECT ID ");
                    sb.Append("FROM Hashtags ");
                    sb.Append(String.Format("WHERE Name=N'{0}';", hashtag));
                    String sql = sb.ToString();

                    SqlCommand command = new SqlCommand(sql, connection);
                    SqlDataReader reader = command.ExecuteReader();
                    if (reader.HasRows)
                    {
                        while (reader.Read())
                        {
                            result.Append(reader.GetInt32(0));
                        }
                        reader.Close();
                    }
                    connection.Close();
                }
                return Int32.Parse(result.ToString());
            }

            // Gets ID of language
            int GetLanguageID(string language)
            {
                StringBuilder result = new StringBuilder();

                using (SqlConnection connection = new SqlConnection(cb.ConnectionString))
                {
                    connection.Open();

                    StringBuilder sb = new StringBuilder();
                    sb.Append("SELECT ID ");
                    sb.Append("FROM Languages ");
                    sb.Append(String.Format("WHERE Name='{0}';", language));
                    String sql = sb.ToString();

                    SqlCommand command = new SqlCommand(sql, connection);
                    SqlDataReader reader = command.ExecuteReader();
                    if (reader.HasRows)
                    {
                        while (reader.Read())
                        {
                            result.Append(reader.GetInt32(0));
                        }
                        reader.Close();
                    }
                    connection.Close();
                }
                return Int32.Parse(result.ToString());
            }

            // Checks DB for existing Hastag with same name
            string GetHashtag (string hashtag)
            {
                StringBuilder result = new StringBuilder();

                using (SqlConnection connection = new SqlConnection(cb.ConnectionString))
                {
   
                    connection.Open();

                    StringBuilder sb = new StringBuilder();
                    sb.Append("SELECT 1 ");
                    sb.Append("FROM Hashtags ");
                    sb.Append(String.Format("WHERE Name=N'{0}';", hashtag));
                    String sql = sb.ToString();

                    SqlCommand command = new SqlCommand(sql, connection);
                    SqlDataReader reader = command.ExecuteReader();
                    if (reader.HasRows)
                    {
                        while (reader.Read())
                        {
                            result.Append(reader.GetInt32(0));
                        }
                        reader.Close();
                    }
                    connection.Close();
                }
                return result.ToString();
            }

            // Checks DB for existing language with same name
            string GetLanguage(string language)
            {
                StringBuilder result = new StringBuilder();

                using (SqlConnection connection = new SqlConnection(cb.ConnectionString))
                {
                    connection.Open();

                    StringBuilder sb = new StringBuilder();
                    sb.Append("SELECT 1 ");
                    sb.Append("FROM Languages ");
                    sb.Append(String.Format("WHERE Name='{0}';", language));
                    String sql = sb.ToString();

                    SqlCommand command = new SqlCommand(sql, connection);
                    SqlDataReader reader = command.ExecuteReader();
                    if (reader.HasRows)
                    {
                        while (reader.Read())
                        {
                            result.Append(reader.GetInt32(0));
                        }
                        reader.Close();
                    }
                    connection.Close();
                }
                return result.ToString();
            }
        }
    }
}
