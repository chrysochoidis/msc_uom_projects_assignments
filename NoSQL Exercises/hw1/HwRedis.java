import java.io.*;
import java.util.*;
import redis.clients.jedis.Jedis;

public class HwRedis {

	public static void main (String [] args) throws Exception {
	 String replyFromUser;
        String user;
        String longUrl = "";
        String shortUrl = "";
        Jedis jedis = new Jedis();

        // read from the input
        BufferedReader inFromUser = new BufferedReader(new InputStreamReader(System.in));

        while (true) {
            System.out.println("(I)nsert a long URL | (Q)uery a short URL | (S)tatistics | e(X)it");
            replyFromUser = inFromUser.readLine(); //read the reply

            if (replyFromUser.equals("I")) {
                System.out.println("Enter your name: ");
                user = inFromUser.readLine();

                System.out.println("Enter a long url: ");
                longUrl = inFromUser.readLine();
                shortUrl = jedis.hget("longToShort", longUrl);

                if(shortUrl != null){
                    System.out.println("Ths url already exists. Its short url is : " + shortUrl);
                }
                else{
                    shortUrl = generateShortUrl();

                    jedis.hset("longToShort", longUrl, shortUrl);

                    Map<String, String> shortUrlData = new HashMap<>();
                    shortUrlData.put("longUrl", longUrl);
                    shortUrlData.put("user", user);
                    shortUrlData.put("queryCount", "0");

                    jedis.hset("shortUrl:" + shortUrl, shortUrlData);
                    jedis.hincrBy("user:" + user, "urlsCreated", 1);
                    System.out.println("Short url has been created!");
                }

            } else if (replyFromUser.equals("Q")) {
                System.out.println("Enter a short Url (6 char code): ");
                shortUrl = inFromUser.readLine();

                if(jedis.exists("shortUrl:" + shortUrl)){
                    longUrl = jedis.hget("shortUrl:" + shortUrl, "longUrl");

                    //increase the query count by 1
                    jedis.hincrBy("shortUrl:" + shortUrl, "queryCount", 1);

                    System.out.println("The long url is : " + longUrl);
                }
                else{
                    System.out.println("This short url doesn't exist.");
                }
            } else if (replyFromUser.equals("S")) {
                System.out.println("Statistics :");

                Set<String> users = jedis.keys("user:*");
                for(String u : users){
                    String name = u.substring("User:".length());
                    String urlsCreated = jedis.hget(u, "urlsCreated");
                    System.out.println(u + " has inserted " + urlsCreated + " urls.");
                }

                int sum = 0;
                int count = 0;
                double avg;

                Set<String> shorts = jedis.keys("shortUrl:*");
                for(String s : shorts){
                    sum += Integer.parseInt(jedis.hget(s, "queryCount"));
                    count++;
                }
                avg = (double)sum / count;
                System.out.println("Average query count is : " + avg);

            } else if (replyFromUser.equals("X")) {
                System.out.println("Goodbye");
                jedis.close();
                System.exit(1);
            } else {
                System.out.println(replyFromUser + "is not a valid choice, retry");
            }
        }
    }

    public static String generateShortUrl(){
        String AB = "0123456789abcdefghijklmnopqrstuvwxyz";
        int len = 6;
        Random rnd = new Random();
        StringBuilder sb = new StringBuilder( len );
        for( int i = 0; i < len; i++ )
            sb.append( AB.charAt( rnd.nextInt(AB.length()) ) );
        return sb.toString();
    }
}
