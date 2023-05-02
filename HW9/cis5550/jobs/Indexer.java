package cis5550.jobs;

import cis5550.flame.FlameContext;
import cis5550.flame.FlamePair;
import cis5550.flame.FlamePairRDD;


import java.util.*;

public class Indexer {
    public static void run(FlameContext flameContext, String[] args) throws Exception {
        FlamePairRDD cd = flameContext.fromTable("crawl", row -> row.get("url") + "," + row.get("page"))
                .mapToPair(a -> {
                    int x = a.indexOf(',');
                    String url = a.substring(0, x);
                    String page = a.substring(x);
         return new FlamePair(url, page);
        });
        final long totalDocuments = flameContext.getKVS().count("crawl");

        FlamePairRDD fullIndex = cd.flatMapToPair((FlamePair fp) -> {
            String url = fp._1();
            String page = fp._2();

            String simplifiedPage = page.replaceAll("<[^>]*>", " ").replaceAll("[.,:;!?’\"()-]", " ");
            Vector<FlamePair> fpList = new Vector<>();
            String[] pageContent = simplifiedPage.split("(\\W)+");
            for(int i = 0; i < pageContent.length; i++){
                String word = pageContent[i];
                word = word.toLowerCase().trim();
                if(word == ""){
                    continue;
                }
                Stemmer stemmer = new Stemmer();
                for(char c : word.toCharArray()){
                    stemmer.add(c);
                }
                stemmer.stem();

                String stem = stemmer.toString();
                String urlTerm = url + ":" + i;
                if(!word.equals(stem)){
                    fpList.add(new FlamePair(stem, urlTerm));
                }
                fpList.add(new FlamePair(word, urlTerm));
            }
            HashMap<String, String> allURLs = new HashMap<>();
            for(FlamePair flamePair : fpList) {
                String word = flamePair._1();
                String urlIn = flamePair._2();
                String urlOnly = urlIn.substring(0, urlIn.lastIndexOf(':'));
                String val = urlIn.substring(urlIn.lastIndexOf(':')+1);
                if(allURLs.containsKey(word+";"+urlOnly)){
                    String listVals = allURLs.get(word + ";" + urlOnly);
                    allURLs.put(word + ";" + urlOnly, listVals + " " +  val);
                }else{
                    allURLs.put(word + ";" + urlOnly, val);
                }
            }
            ArrayList<FlamePair> flamePairs = new ArrayList<>();
            for(String keys : allURLs.keySet()){
                String[] twoParts = keys.split(";",2);
                String wordPart = twoParts[0];

                String urlPart = twoParts[1];
                String list = allURLs.get(keys);
                flamePairs.add(new FlamePair(wordPart, urlPart + ":" + list));
            }
            return flamePairs;
        });
        FlamePairRDD reversedIndex = fullIndex.foldByKey("", (String s, String s2) -> {
            return s.equals("") ? s2 : s + "," + s2;
        });

        /*FlamePairRDD aggregateLinks = reversedIndex.flatMapToPair((FlamePair fp) ->{
            String word = fp._1();
            String[] urls = fp._2().split(",");

            HashMap<String, String> vals = new HashMap<>();
            ArrayList<String> allJoined = new ArrayList<>();
            //System.out.print(word + " - ");
            for(String url : urls){
                String urlOnly = url.substring(0, url.lastIndexOf(':'));
                String val = url.substring(url.lastIndexOf(':')+1);
                //System.out.print("("+ urlOnly + ")"+val + " ");
                if(vals.containsKey(urlOnly)){
                    String link = vals.get(urlOnly);
                    vals.put(urlOnly, link + " " + val);
                }else{
                    vals.put(urlOnly, val);
                }
            }
            //System.out.println();
            for(String key : vals.keySet()){
                allJoined.add(key + ":" + vals.get(key));
            }
            return Collections.singletonList(new FlamePair(word, String.join(",", allJoined)));
        });*/
        // Sorting
        FlamePairRDD sortLinks = reversedIndex.flatMapToPair((flamePair) -> {
            String word = flamePair._1();
            String[] urls = flamePair._2().split(",");
            ArrayList<String> allUrls = new ArrayList<>();
            for(String url : urls){
                allUrls.add(url);
            }
            int docFrequency = urls.length;
            assert docFrequency != 0;

            Double idf = totalDocuments != 0 ? Math.log10(totalDocuments/docFrequency) : 0;
            flameContext.getKVS().put("idfRanks", word, "IDF", String.valueOf(idf));
            Collections.sort(allUrls, new URLComparator());

            return Collections.singletonList(new FlamePair(word, String.join(",", allUrls)));
        });

        sortLinks.saveAsTable("index");
        flameContext.getKVS().persist("index");

        flameContext.output("OK");
    }
    static final class URLComparator implements Comparator<String>
    {
        public int compare(String a, String b)
        {
            String lstA = a.substring(a.lastIndexOf(':'));
            String lstB = b.substring(b.lastIndexOf(':'));
            int lenA = lstA.split(" ").length;
            int lenB = lstB.split(" ").length;
            if(lenA > lenB){
                return -1;
            }else if(lenA < lenB){
                return 1;
            }else{
                return 0;
            }
        }
    }

}
