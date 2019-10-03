import java.time.Instant;
import java.time.temporal.ChronoUnit;
/*from   www.  jav a2 s  .c o  m*/
public class Main {

  public static void main(String[] args) {
    Instant t1 = Instant.now();
    long hours = 2;
    long minutes = 30;
    Instant t2 = t1.plus(hours, ChronoUnit.HOURS).plus(minutes, ChronoUnit.MINUTES);
    
    Duration gap = Duration.ofSeconds(13);
    Instant later = t1.plus(gap);
    System.out.println(later);
    
    System.out.println(ChronoUnit.MILLIS.between(t1, t2));  
  }
}