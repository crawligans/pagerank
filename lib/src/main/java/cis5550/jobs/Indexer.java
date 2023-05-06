package cis5550.jobs;

import cis5550.flame.FlameContext;
import cis5550.flame.FlameContextImpl;
import cis5550.flame.FlamePair;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Hasher;
import cis5550.tools.Logger;
import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Vector;

public class Indexer {

    private static Logger logger = Logger.getLogger(Indexer.class);

    public static void run(FlameContext flameContext, String[] args) throws Exception {
        final long totalDocuments = flameContext.getKVS().count("crawl");
        FlameContextImpl ctx = (FlameContextImpl) flameContext;
        flameContext.getKVS().persist("TF");
        flameContext.fromTable("crawl", crawlRow -> {
            String url = Optional.ofNullable(crawlRow.get("url"))
                .map(l -> l.replaceAll(",", URLEncoder.encode(",", StandardCharsets.UTF_8)))
                .orElse(null);
            String page = crawlRow.get("page");

            if (url == null || page == null) {
                return "";
            }

            String simplifiedPage = page.replaceAll("<[^>]*>", " ")
                .replaceAll("[.,:;!?’\"()-]", " ");
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
                    return v;
                });
            }

            KVSClient kvs = flameContext.getKVS();
            String urlHash = Hasher.hash(url);
            Row row = new Row(urlHash);
            counts.forEach((k, v) -> row.put(k, String.valueOf(v.size())));
            try {
                kvs.putRow("TF", row);
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
            }

            try {
                ctx.parallelizePairs("indexUnsorted", counts.entrySet().stream().map(
                    e -> new FlamePair(e.getKey(), url + ":" + String.join(" ",
                        () -> e.getValue().stream().map(String::valueOf)
                            .map(CharSequence.class::cast).iterator()))));
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
            return "";
        }).drop();

        // Sorting
        flameContext.getKVS().persist("IDF");
        flameContext.getKVS().persist("index");
        flameContext.fromTable("indexUnsorted", row -> {
            String word = row.key();
            String[] urls = row.columns().stream().map(row::get).sorted(new URLComparator())
                .toArray(String[]::new);
            int docFrequency = urls.length;
            assert docFrequency != 0;

            Double idf =
                totalDocuments != 0 ? Math.log10(((double) totalDocuments) / docFrequency) : 0;
            try {
                flameContext.getKVS().put("IDF", word, "IDF", String.valueOf(idf));
                flameContext.getKVS().put("index", word, "link", String.join(",", urls));
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
            }
            return "";
        }).drop();

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
