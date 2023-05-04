package cis5550.jobs;

import cis5550.flame.FlameContext;
import cis5550.flame.FlamePair;
import cis5550.flame.FlamePairRDD;
import cis5550.flame.FlameRDD;
import cis5550.tools.URLParser;

import java.net.URL;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import cis5550.jobs.Stemmer;

public class PageRank {
    public static void run(FlameContext flameContext, String[] args) throws Exception {
        flameContext.getKVS().persist("pageranks");
        if(args.length < 1 || args.length == 2){
            flameContext.output("FAIL");
            return;
        }

        boolean extraConverganceCriteria = false;
        if(args.length == 3){
            extraConverganceCriteria = true;
        }
        final double threshold = Double.parseDouble(args[0].trim());
        final double decayFactor = 0.85;
        System.out.println(threshold);
        FlamePairRDD stateTable = flameContext.fromTable("crawl", row -> row.get("url") + "," + row.get("page"))
                .mapToPair(a -> {

                        int x = a.indexOf(',');
                        String url = a.substring(0, x);

                        String page = a.substring(x);
                        List<String> extracted = extractURLS(page);
                        List<String> normalisedExtracted = normaliseURLS(extracted, new URL(url));
                        String allLinks = String.join(",", normalisedExtracted);
                        return new FlamePair(url, "1.0,1.0," + allLinks);
                });
        FlamePairRDD transferTable;

        int iterations = 0;
        while(true){
            iterations++;
            transferTable = stateTable.flatMapToPair( (FlamePair flamePair) -> {
                String srcURL = flamePair._1();
                if(srcURL.equals("null") || srcURL == null){
                    return Collections.emptyList();
                }
                String l = flamePair._2();
                String[] splitSt = l.split(",", 3);
                String[] links = splitSt[2].split(",");

                int n = links.length;
                // current,prev, links
                // int n = splitSt.length-2;
                double currentRank = Double.parseDouble(splitSt[0]);
                ArrayList<FlamePair> assignedRanks = new ArrayList<>();
                HashSet<String> seenURLS = new HashSet<String>();
                assignedRanks.add(new FlamePair(srcURL, Double.toString(0.0)));
                Double rank = decayFactor * currentRank / n;
                for(String linkedURL : links){
                    if(seenURLS.contains(linkedURL) || linkedURL.isBlank()){
                        continue;
                    }
                    assignedRanks.add(new FlamePair(linkedURL, rank.toString()));
                    seenURLS.add(linkedURL);
                    //System.out.println(splitSt[i] + "," + rank);
                }
                return assignedRanks;
            }).foldByKey("0.0", (String s, String s2) -> {
                double liftS = Double.parseDouble(s);
                double liftS2 = Double.parseDouble(s2);
                return Double.toString(liftS + liftS2);
            });
            FlamePairRDD nextState = stateTable.join(transferTable).flatMapToPair((flamePair)->
            {
                String n = flamePair._2();
                String[] c = n.split(",");
                ArrayList<String> links = new ArrayList<>();
                int index = c.length - 1;

                for(int i = 2; i < index; i++){
                    links.add(c[i]);
                }
                String oldRank = c[0];
                String newRank = c[index];

                // current, prev, links, new
                FlamePair fp  = new FlamePair(flamePair._1(),
                        (Double.parseDouble(newRank) + (1 - decayFactor))+ "," + oldRank + "," + String.join(",", links));
                return Collections.singleton(fp); // Add 0.15 from the rank source
            });

            FlameRDD diffTable = nextState.flatMap(sd -> {
                String c = sd._2();
                String[] clin = c.split(",", 3);
                double rc = Double.parseDouble(clin[0]);
                double rp = Double.parseDouble(clin[1]);
                return Collections.singletonList(Double.toString(Math.abs(rp-rc)));
            });
            // Extra convergence criteria only set if args.length == 3
            if(extraConverganceCriteria){
                final double defRankDiff = Double.parseDouble(args[1]);
                double  percentConv = Double.parseDouble(args[2]);
                double defPercentConv = (percentConv > 0) && (percentConv < 1) ? percentConv : (percentConv / 100);
                String totalConverged = diffTable.fold("0", (left, right) -> {
                    double difference = Double.parseDouble(right);
                    int currentCount = Integer.parseInt(left);
                    if(difference < defRankDiff){
                        currentCount++;
                    }
                    return Integer.toString(currentCount);
                });
                if(Integer.parseInt(totalConverged) >= defPercentConv * diffTable.count()){
                    break;
                }
            }

            String lca = diffTable.fold("0.0", (left, right) -> {
                double acc = Double.parseDouble(left);
                double val = Double.parseDouble(right);
                return Double.toString(Math.max(acc, val));
            });
           if(Double.parseDouble(lca) < threshold){
               break;
           }

           stateTable = nextState;
        }
        System.out.println("Converged in: " + iterations + " iterations");
        stateTable.flatMapToPair(flamePair -> {
            String url  = flamePair._1();
            String right = flamePair._2();
            int i = right.indexOf(",");
            double rank = Double.parseDouble(right.substring(0, i));
            flameContext.getKVS().put("pageranks", url, "rank", Double.toString(rank));
            return Collections.emptyList();
        });

        flameContext.output("OK");
    }
    private static List<String> normaliseURLS(List<String> urlList, URL seedURL) {
        try {
            String defaultPort = Integer.toString(seedURL.getDefaultPort());
            String protocol = seedURL.getProtocol() + "://";
            String hostName = seedURL.getHost();
            String urlRedirect = seedURL.getPath();
            String portNo = Integer.toString(seedURL.getPort());

            ArrayList<String> normalisedURLs = new ArrayList<>();
            urlList = urlList.stream()
                    .map(s -> s.replaceAll("(#(.*?)(?=/))|(#(.*?)\\z)", "")) // has internal links
                    .filter(s -> {
                        for(String k : List.of(".txt", ".png", ".jpg", ".jpeg", ".gif")){
                            if(s.endsWith(k)) return false;
                        }
                        return true;
                    })
                    .collect(Collectors.toList());
            for (String url : urlList) {
                //path leads elsewhere
                if(url.startsWith("http")){
                    String[] swa = URLParser.parseURL(url);
                    if(swa[2] == null){
                        swa[2] = defaultPort;
                    }
                    normalisedURLs.add(swa[0] + "://" + swa[1] + ":" + swa[2] + swa[3]);
                    continue;
                }

                // path is relative
                if (url.startsWith("..")) {
                    String[] str = urlRedirect.split("/");
                    String[] sl = url.split("/");
                    int i = str.length - 1;

                    int j = 0;
                    for(; i > 0; i--){
                        if(j <= sl.length - 1){
                            if(!sl[j].equals("..")){
                                break;
                            }
                            j++;
                        }else{
                            break;
                        }
                    }
                    StringBuilder sb = new StringBuilder();
                    if(i == 0){
                        sb.append("/");
                    }else{
                        for(int k = 1; k <= i-1; k++){
                            sb.append("/"+str[k]);
                        }
                        for(int k = j; k < sl.length; k++){
                            sb.append("/" + sl[k]);
                        }
                    }
                    url = sb.toString();

                }
                // is absolute
                if(url.startsWith("/")){
                    url = protocol + hostName + ":" + portNo + url;
                }else{
                    String substr = urlRedirect.substring(0, urlRedirect.lastIndexOf('/'));
                    url = protocol + hostName + ":" + portNo + substr + "/" + url;
                }
                normalisedURLs.add(url);
            }
            return normalisedURLs;
        }catch(Exception e){
            e.printStackTrace();
        }
        return null;
    }
    private static List<String> extractURLS(String content) {
        Pattern pattern = Pattern.compile("<[^/](.*?)>");
        Matcher m = pattern.matcher(content);
        HashSet<String> urlList = new HashSet<String>();
        while(m.find()){

            String entered = content.substring(m.start()+1, m.end());
            String[] sre = entered.split(" ");

            if(sre[0].equalsIgnoreCase("a")){
                if(sre[1].startsWith("href")){
                    urlList.add(sre[1].substring(sre[1].indexOf("\"")+1, sre[1].length()-2));
                }
            }
        }
        return urlList.stream().collect(Collectors.toList());
    }
}
