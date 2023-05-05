package cis5550.jobs;

import cis5550.flame.FlameContext;
import cis5550.flame.FlamePair;
import cis5550.flame.FlamePairRDD;
import cis5550.kvs.KVSClient;
import cis5550.tools.Hasher;
import java.io.IOException;
import java.net.URLEncoder;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

public class Indexer {
    public static void run(FlameContext flameContext, String[] args) throws Exception {
        flameContext.getKVS().persist("IDF");
        flameContext.getKVS().persist("TF");
        flameContext.getKVS().persist("index");
        FlamePairRDD cd = flameContext.fromTable("crawl",
                row -> row.get("url").replaceAll(",", URLEncoder.encode(",")) + "," + row.get("page"))
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

            String simplifiedPage = page.replaceAll("<[^>]*>", " ")
                .replaceAll("[.,:;!?’\"()-]", " ");
            Vector<FlamePair> fpList = new Vector<>();
            Map<String, List<Integer>> counts = new HashMap<>();
            String[] pageContent = simplifiedPage.split("(\\W)+");
            for (int i = 0; i < pageContent.length; i++) {
                String word = pageContent[i];
                word = word.toLowerCase().trim();
                if (word.isBlank()) {
                    continue;
                }
                Stemmer stemmer = new Stemmer();
                for (char c : word.toCharArray()) {
                    stemmer.add(c);
                }
                stemmer.stem();

                String stem = stemmer.toString();

                final int finalI = i;
                counts.compute(stem, (k, v) -> {
                    if (v == null) {
                        v = new Vector<>();
                    }
                    v.add(finalI);
                    return null;
                });
            }

            KVSClient kvs = flameContext.getKVS();
            String urlHash = Hasher.hash(url);

            return () -> counts.entrySet().parallelStream().peek(e -> {
                try {
                    kvs.put("TF", urlHash, "__url", url);
                    kvs.put("TF", urlHash, e.getKey(), String.valueOf(e.getValue().size()));
                } catch (IOException ignored) {
                }
            }).map(e -> new FlamePair(e.getKey(), String.join(" ",
                () -> e.getValue().stream().map(String::valueOf).map(CharSequence.class::cast)
                    .iterator()))).iterator();
        });
        FlamePairRDD reversedIndex = fullIndex.foldByKey("",
            (s, s2) -> s.isBlank() ? s2 : s + "," + s2);

        // Sorting
        FlamePairRDD sortLinks = reversedIndex.flatMapToPair((flamePair) -> {
            String word = flamePair._1();
            String[] urls = flamePair._2().split(",");
            Arrays.sort(urls, new URLComparator());

            int docFrequency = urls.length;
            assert docFrequency != 0;

            Double idf =
                totalDocuments != 0 ? Math.log10(((double) totalDocuments) / docFrequency) : 0;
            flameContext.getKVS().put("IDF", word, "IDF", String.valueOf(idf));

            return Collections.singletonList(new FlamePair(word, String.join(",", urls)));
        });

        sortLinks.flatMapToPair(flamePair -> {
            flameContext.getKVS().put("index", flamePair._1(), "link", flamePair._2());
            return Collections::emptyIterator;
        });

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
            return Integer.compare(lenB, lenA);
        }
    }

}
