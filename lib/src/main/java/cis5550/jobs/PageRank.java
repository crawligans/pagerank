package cis5550.jobs;

import cis5550.flame.FlameContext;
import cis5550.flame.FlamePair;
import cis5550.flame.FlamePairRDD;
import cis5550.flame.FlameRDD;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Scanner;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class PageRank {

    private static final Pattern anchorTag = Pattern.compile(
        "<(?:a|img)((\\s+[-\\w]+=\"[^\"]*\")*)\\s*>");

    public static void run(FlameContext flameContext, String[] args) throws Exception {
        flameContext.getKVS().persist("pageranks");
        if (args.length < 1 || args.length == 2) {
            flameContext.output("FAIL");
            return;
        }

        boolean extraConverganceCriteria = args.length == 3;
        final double threshold = Double.parseDouble(args[0].trim());
        final double decayFactor = 0.85;
        System.out.println(threshold);
        FlamePairRDD stateTable = flameContext.fromTable("crawl", row -> {
            try {
                return "%s,1.0,1.0,%s".formatted(normalizeUrl(new URL(row.get("url"))),
                    new Scanner(row.get("page")).findAll(anchorTag).map(m -> m.group(1))
                        .flatMap(props -> Arrays.stream(props.split("\\s+")))
                        .map(prop -> prop.split("="))
                        .filter(prop -> "href".equals(prop[0])).map(prop -> prop[1].strip())
                        .map(propV -> propV.substring(1, propV.length() - 1)).map(propV -> {
                            try {
                                return normalizeUrl(new URL(new URL(row.get("url")), propV)).toString();
                            } catch (MalformedURLException e) {
                                return null;
                            }
                        }).filter(Objects::nonNull).collect(Collectors.joining(",")));
            } catch (MalformedURLException e) {
                return row.get("url") + ",1.0,1.0,";
            }
        }).mapToPair(e -> {
            String[] s = e.split(",", 2);
            return new FlamePair(s[0], s[1]);
        });
        FlamePairRDD transferTable;

        int iterations = 0;
        while (true) {
            iterations++;
            transferTable = stateTable.flatMapToPair((FlamePair flamePair) -> {
                String srcURL = flamePair._1();
                if (srcURL.equals("null") || srcURL.isBlank()) {
                    return Collections::emptyIterator;
                }
                String l = flamePair._2();
                String[] splitSt = l.split(",", 3);
                String[] links = splitSt[2].split(",");

                int n = links.length;
                // current,prev, links
                // int n = splitSt.length-2;
                double currentRank = Double.parseDouble(splitSt[0]);
                ArrayList<FlamePair> assignedRanks = new ArrayList<>();
                HashSet<String> seenURLS = new HashSet<>();
                assignedRanks.add(new FlamePair(srcURL, "0.0"));
                double rank = decayFactor * currentRank / n;
                for (String linkedURL : links) {
                    if (seenURLS.contains(linkedURL) || linkedURL.isBlank()) {
                        continue;
                    }
                    assignedRanks.add(new FlamePair(linkedURL, Double.toString(rank)));
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
            String url = flamePair._1();
            String right = flamePair._2();
            int i = right.indexOf(",");
            double rank = Double.parseDouble(right.substring(0, i));
            flameContext.getKVS().put("pageranks", url, "rank", Double.toString(rank));
            return Collections.emptyList();
        });

        flameContext.output("OK");
    }

    public static URL normalizeUrl(URL url) throws MalformedURLException {
        String protocol = url.getProtocol();
        String host = url.getHost();
        int port = url.getPort() < 0 ? url.getDefaultPort() : url.getPort();
        String file = url.getPath();
        return new URL(protocol, host, port, "".equals(file) ? "/" : file);
    }

    private static List<String> normaliseURLS(List<String> urlList, URL seedURL) {
        return urlList.stream()
            .map(s -> s.replaceAll("(#(.*?)(?=/))|(#(.*?)\\z)", "")) // has internal links
            .filter(s -> {
                for (String k : List.of(".txt", ".png", ".jpg", ".jpeg", ".gif")) {
                    if (s.endsWith(k)) {
                        return false;
                    }
                }
                return true;
            }).map(s -> {
                try {
                    return normalizeUrl(new URL(seedURL, s));
                } catch (MalformedURLException e) {
                    return null;
                }
            }).filter(Objects::nonNull).map(URL::toExternalForm).toList();
    }

    private static List<String> extractURLS(String content) {
        return new Scanner(content).findAll(anchorTag).map(m -> m.group(1)).filter(Objects::nonNull)
            .flatMap(props -> Arrays.stream(props.split("\\s+"))).map(prop -> prop.split("="))
            .filter(prop -> prop.length >= 2).filter(prop -> "href".equals(prop[0]))
            .map(prop -> prop[1].strip())
            .filter(propV -> propV.startsWith("\"") && propV.endsWith("\""))
            .map(propV -> propV.substring(1, propV.length() - 1)).toList();
    }
}
