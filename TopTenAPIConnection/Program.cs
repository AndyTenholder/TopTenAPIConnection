using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tweetinvi;
using System.Data.SqlClient;   // System.Data.dll 
//using System.Data;           // For:  SqlDbType , ParameterDirection


namespace TopTenAPIConnection
{
    class Program
    {
        static void Main(string[] args)
        {
            try
            {
                SqlConnectionStringBuilder cb = new SqlConnectionStringBuilder();
                cb.DataSource = "****************"; ;
                cb.UserID = "****************";
                cb.Password = "****************"; ;
                cb.InitialCatalog = "****************"; ;
                // Set up your credentials (https://apps.twitter.com)
                // Applies credentials for the current thread.If used for the first time, set up the ApplicationCredentials
                Auth.SetUserCredentials("" * ***************";", "" * ***************";", "" * ***************";-" * ***************";", "" * ***************";");
                var user = User.GetAuthenticatedUser();

                // Enable Automatic RateLimit handling
                //RateLimit.RateLimitTrackerMode = RateLimitTrackerMode.TrackAndAwait;

                var stream = Stream.CreateSampleStream();


                /* Using Async version of StartStreamMatchingAnyCondition method
                 * without Async the API stream will hold up the stack
                 * shifting it onto another thread allows host.run() to be called 
                 * and the web app to run normally
                 */
                stream.StartStreamAsync();

                stream.StreamStopped += (sender, argues) =>
                {
                    stream.ResumeStream();
                };

                stream.TweetReceived += (sender, recievedTweet) =>
                {
                    if (recievedTweet.Tweet.Hashtags.Count() > 0)
                    {
                        DateTime timeNow = DateTime.Now;
                        string tweetLanguage = recievedTweet.Tweet.Language.ToString();

                        // if language is not in DB add it
                        if (GetLanguage(recievedTweet.Tweet.Language.ToString(), cb) == "")
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
                            }
                        }

                        // Create new tweet object and add to db

                        using (var connection = new SqlConnection(cb.ConnectionString))
                        {
                            connection.Open();
                            StringBuilder sb = new StringBuilder();
                            sb.Append("INSERT INTO Tweets (DateTime, LanguageID) ");
                            sb.Append(String.Format("VALUES ('{0}' , {1});", timeNow.ToString(), GetLanguageID(tweetLanguage, cb)));
                            String sql = sb.ToString();

                            using (var command = new SqlCommand(sql, connection))
                            {
                                int rowsAffected = command.ExecuteNonQuery();
                            }
                        }

                        List<string> hashtagList = new List<string>();

                        // if hashtag is not in DB add it
                        foreach (var hashtag in recievedTweet.Tweet.Hashtags)
                        {
                            // Convert hashtag to uppercase string
                            var upperHashtag = hashtag.ToString().ToUpper();

                            if (GetHashtag(upperHashtag, cb) != "")
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
                                    sb.Append(String.Format("VALUES ('{0}');", upperHashtag));
                                    String sql = sb.ToString();

                                    using (var command = new SqlCommand(sql, connection))
                                    {
                                        command.ExecuteNonQuery();
                                        Console.WriteLine("Added Hashtag {0} to database.", upperHashtag);
                                    }
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
                            using (var connection = new SqlConnection(cb.ConnectionString))
                            {
                                connection.Open();
                                StringBuilder sb = new StringBuilder();
                                sb.Append("INSERT INTO TweetHashtags (HashtagID, TweetID) ");
                                sb.Append(String.Format("VALUES ('{0}' , {1});", GetHashtagID(hashtag.ToString().ToUpper(),cb), GetTweetID(tweetLanguage, timeNow, cb)));
                                String sql = sb.ToString();

                                using (var command = new SqlCommand(sql, connection))
                                {
                                    
                                }
                            }
                        }
                    }
                };
            }
            catch (SqlException e)
            {
                Console.WriteLine(e.ToString());
            }
            Console.ReadKey();

            // Gets ID of Tweet
            string GetTweetID(string language,DateTime dateTime, SqlConnectionStringBuilder cb)
            {
                StringBuilder result = new StringBuilder();

                using (SqlConnection connection = new SqlConnection(cb.ConnectionString))
                {
                    connection.Open();

                    StringBuilder sb = new StringBuilder();
                    sb.Append("SELECT ID ");
                    sb.Append("FROM Tweets ");
                    sb.Append(String.Format("WHERE DateTime='{0}' ", dateTime.ToString())); 
                    sb.Append(String.Format("AND LanguageID ={0};", GetLanguageID(language, cb)));
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
                }
                return result.ToString();
            }

            // Gets ID of Hashtag
            string GetHashtagID(string hashtag, SqlConnectionStringBuilder cb)
            {
                StringBuilder result = new StringBuilder();

                using (SqlConnection connection = new SqlConnection(cb.ConnectionString))
                {
                    connection.Open();

                    StringBuilder sb = new StringBuilder();
                    sb.Append("SELECT ID ");
                    sb.Append("FROM Hashtags ");
                    sb.Append(String.Format("WHERE Name='{0}';", hashtag));
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
                }
                return result.ToString();
            }

            // Gets ID of language
            string GetLanguageID(string language, SqlConnectionStringBuilder cb)
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
                }
                return result.ToString();
            }

            // Checks DB for existing Hastag with same name
            string GetHashtag (string hashtag, SqlConnectionStringBuilder cb)
            {
                StringBuilder result = new StringBuilder();

                using (SqlConnection connection = new SqlConnection(cb.ConnectionString))
                {
   
                    connection.Open();

                    StringBuilder sb = new StringBuilder();
                    sb.Append("SELECT 1 ");
                    sb.Append("FROM Hashtags ");
                    sb.Append(String.Format("WHERE Name='{0}';", hashtag));
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
                }
                return result.ToString();
            }

            // Checks DB for existing language with same name
            string GetLanguage(string language, SqlConnectionStringBuilder cb)
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
                }
                return result.ToString();
            }
        }
    }
}
