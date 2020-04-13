#Import the necessary methods from tweepy library
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

#Variables that contains the user credentials to access Twitter API 
access_token = "1323043225-H5E4NJAyI1Z4W7FXD9KYdwJe4TO26x6QuoD5nPK"
access_token_secret = "VNUPdDBlBPxguBLohrBXM4IsvaH0FAZVrIsJ3kcWRJCf7"
consumer_key = "iAt7lG9Sizsv7YbfCFZYJdksg"
consumer_secret = "8ceIdGUAZO2g93EsmBjwiqclfTjKOaNhZkjrZxZEsbUMbFp9Im"


#This is a basic listener that just prints received tweets to stdout.
class StdOutListener(StreamListener):

    def on_data(self, data):
        # print (data)
        return True

    def on_error(self, status):
        print (status)


if __name__ == '__main__':

    #This handles Twitter authetification and the connection to Twitter Streaming API
    l = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, l)
    queries = ["everyday", "everyday lite", "everyday light", "everydaylite", "everydaylight",
           "evryday light", "evryday lite", "tea", "chai", "powdered milk", "powder milk", "powdermilk", "pwdr milk",
           "lipton", "tapal","danedar", "danaydar", "khaas lite", "patti"]
    #This line filter Twitter Streams to capture data by the keywords: 'python', 'javascript', 'ruby'
    stream.filter(track= queries)

    # stream.filter(locations=[-122.4440, 47.4792, -122.2421, 47.7592])