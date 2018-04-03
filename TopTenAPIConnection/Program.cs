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
using TopTenAPIConnection.Models;
//using System.Data;           // For:  SqlDbType , ParameterDirection


namespace TopTenAPIConnection
{
    class Program
    {
        static void Main(string[] args)
        {
            ServicePointManager.SecurityProtocol = SecurityProtocolType.Ssl3 | SecurityProtocolType.Tls | SecurityProtocolType.Tls12 | SecurityProtocolType.Tls11;

            SqlConnectionStringBuilder cb = new SqlConnectionStringBuilder();
            cb.DataSource = "toptenhashtags-server.database.windows.net";
            cb.UserID = "AndyTenholder@toptenhashtags-server";
            cb.Password = "DeadDudesDontTalk1825";
            cb.InitialCatalog = "EfficiencyTest";
            // Set up your credentials (https://apps.twitter.com)
            // Applies credentials for the current thread.If used for the first time, set up the ApplicationCredentials
            Auth.SetUserCredentials("OdPpBONAHJ8ntAdonQwYZnpsA", "U2B15BqG0dsGD6QDBFwcgqwH72N8jOoxv54Fbe4TrLIRHQxF9e", "858816969759444992-crXFkbz9bsbkgxeiUXyfOMizk0C9w5F", "84oGlMZ5FUvv6BEaMhzfBAgxJEL8odVaG7jsL9AVEDFBI");
            var user = User.GetAuthenticatedUser();
            bool twitterCreds1InUse = true;

            // Lists to hold data on known languages and hashtags
            // Eliminate need to hit database in order to 
            List<Language> Languages = new List<Language>();
            List<Hashtag> Hashtags = new List<Hashtag>();

            // Create a language object for each language in the DB and add Language object to Languages List
            using (var connection = new SqlConnection(cb.ConnectionString))
            {
                connection.Open();
                StringBuilder sb = new StringBuilder();
                sb.Append("SELECT * FROM Languages");
                String sql = sb.ToString();
                SqlDataReader dataReader;

                using (var command = new SqlCommand(sql, connection))
                {
                    dataReader = command.ExecuteReader();
                    while(dataReader.Read())
                    {
                        Languages.Add(new Language()
                        {
                            ID = dataReader.GetInt32(dataReader.GetOrdinal("ID")),
                            Name = dataReader.GetString(dataReader.GetOrdinal("Name"))
                        });
                    }
                    dataReader.Close();
                }
                connection.Close();
            }

            // Create a Hashtag object for each Hashtag in the DB and add Hashtag object to Hashtags List
            using (var connection = new SqlConnection(cb.ConnectionString))
            {
                connection.Open();
                StringBuilder sb = new StringBuilder();
                sb.Append("SELECT * FROM Hashtags");
                String sql = sb.ToString();
                SqlDataReader dataReader;

                using (var command = new SqlCommand(sql, connection))
                {
                    dataReader = command.ExecuteReader();
                    while (dataReader.Read())
                    {
                        Hashtags.Add(new Hashtag()
                        {
                            ID = dataReader.GetInt32(dataReader.GetOrdinal("ID")),
                            Name = dataReader.GetString(dataReader.GetOrdinal("Name"))
                        });
                    }
                    dataReader.Close();
                }
                connection.Close();
            }

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
                    var exceptionThatCausedTheStreamToStop = arguems.Exception;
                    Console.WriteLine("------------ Error {0} --------------------", exceptionThatCausedTheStreamToStop);
                    Thread.Sleep(1000);
                    Console.WriteLine("------------ Stream Restarted --------------------");
                    stream.StartStream();
                }
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

                    // Strategy #1 : Wait for RateLimits to be available
                    Console.WriteLine("Waiting for RateLimits until : {0}", queryRateLimits.ResetDateTime.ToLongTimeString());
                    Thread.Sleep((int)queryRateLimits.ResetDateTimeInMilliseconds);

                }
            };

            int tweetcount = 0;

            stream.TweetReceived += (sender, recievedTweet) =>
            {
                tweetcount += 1;
                Console.WriteLine(tweetcount);
                if (recievedTweet.Tweet.Hashtags.Count() > 0)
                {
                    Int32 unixTimestamp = (Int32)(DateTime.UtcNow.Subtract(new DateTime(1970, 1, 1))).TotalSeconds;
                    string tweetLanguageString = recievedTweet.Tweet.Language.ToString();

                    // if language is not in DB add it
                    Language tweetLanguageObject = Languages.SingleOrDefault(l => l.Name == tweetLanguageString);
                    if (tweetLanguageObject == null)
                    {
                        using (var connection = new SqlConnection(cb.ConnectionString))
                        {
                            connection.Open();
                            StringBuilder sb = new StringBuilder();
                            sb.Append("INSERT INTO Languages (Name) ");
                            sb.Append(String.Format("VALUES ('{0}');", tweetLanguageString));
                            String sql = sb.ToString();

                            using (var command = new SqlCommand(sql, connection))
                            {
                                Console.WriteLine("Added Language {0} to database.", tweetLanguageString);
                                command.ExecuteNonQuery();
                            }
                            connection.Close();
                        }
                        Language newLanguage = new Language()
                        {
                            ID = GetLanguageID(tweetLanguageString),
                            Name = tweetLanguageString
                        };
                        Languages.Add(newLanguage);

                        tweetLanguageObject = newLanguage;
                    }

                    StringBuilder stringBuilder = new StringBuilder();
                    for(int x=0; x < recievedTweet.Tweet.Hashtags.Count; x++)
                    {
                        if (x > 0)
                            {
                                stringBuilder.Append("/");
                            }
                        stringBuilder.Append(recievedTweet.Tweet.Hashtags[x].ToString().ToUpper());
                    }
                    string hashtags = stringBuilder.ToString();

                    // Create new tweet object and add to db

                    using (var connection = new SqlConnection(cb.ConnectionString))
                    {
                        connection.Open();
                        StringBuilder sb = new StringBuilder();
                        sb.Append("INSERT INTO Tweets (UnixTimeStamp, LanguageID, TweetIdString, Hashtags) ");
                        sb.Append(String.Format("VALUES ({0} , {1} , '{2}', N'{3}');", unixTimestamp, tweetLanguageObject.ID, recievedTweet.Tweet.IdStr, hashtags));
                        String sql = sb.ToString();

                        using (var command = new SqlCommand(sql, connection))
                        {
                            int rowsAffected = command.ExecuteNonQuery();
                        }
                        connection.Close();
                    }

                    List<Hashtag> hashtagList = new List<Hashtag>();

                    // if hashtag is not in DB add it
                    foreach (var hashtag in recievedTweet.Tweet.Hashtags)
                    {
                        // Convert hashtag to uppercase string
                        var upperHashtag = hashtag.ToString().ToUpper();

                        Hashtag tweetHashtagObject = Hashtags.SingleOrDefault(h => h.Name == upperHashtag);

                        if (tweetHashtagObject != null)
                        {

                            if (!hashtagList.Contains(tweetHashtagObject))
                            {
                                hashtagList.Add(tweetHashtagObject);
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

                                Hashtag newHashtag = new Hashtag()
                                {
                                    ID = GetHashtagID(upperHashtag),
                                    Name = upperHashtag
                                };
                                Hashtags.Add(newHashtag);

                                tweetHashtagObject = newHashtag;
                            }
                            if (!hashtagList.Contains(tweetHashtagObject))
                            {
                                hashtagList.Add(tweetHashtagObject);
                            }
                        }

                    }

                    string TweetID = GetTweetID(recievedTweet.Tweet.IdStr);

                    // Create TweetHashtag object for each hashtag
                    foreach (var hashtag in hashtagList)
                    {
                        
                        if (TweetID.Length > 7)
                        {
                            break;
                        }
                        using (var connection = new SqlConnection(cb.ConnectionString))
                        {
                            connection.Open();
                            StringBuilder sb = new StringBuilder();
                            sb.Append("INSERT INTO TweetHashtags (HashtagID, TweetID, UnixTimeStamp) ");
                            sb.Append(String.Format("VALUES ({0} , '{1}', {2});", hashtag.ID, TweetID, unixTimestamp));
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
