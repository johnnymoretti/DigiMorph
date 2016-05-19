package eu.fbk.dh.digi_morph;


import com.google.common.collect.Lists;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringEscapeUtils;
import org.mapdb.Serializer;
import org.mapdb.SortedTableMap;
import org.mapdb.volume.MappedFileVol;
import org.mapdb.volume.Volume;

import java.io.File;
import java.io.FileReader;
import java.io.Reader;
import java.util.*;
import java.util.concurrent.*;

/**
 *
 * @author Giovanni Moretti at Digital Humanities group at FBK.
 * @version 0.2a
 */
public class DigiMorph {
    String lang = "";
    ExecutorService executor = null;
    List<Future<List<String>>> futures = null;


    Set<Callable<List<String>>> callables = new HashSet<Callable<List<String>>>();


    public static String getVersion(){
        return DigiMorph.class.getPackage().getImplementationTitle()+"\n"
                + DigiMorph.class.getPackage().getSpecificationVendor() + " - "
                + DigiMorph.class.getPackage().getImplementationVendor() + "\n"
                + "Version: "+DigiMorph.class.getPackage().getSpecificationVersion();
    }



    public DigiMorph(String lang) {
        this.lang = lang;
    }

    /**
     *
     * @author Giovanni Moretti
     * @version 0.2a
     * @param token_list list of string containing words.
     * @return  list of string containing the results of the Morphological analyzer.
     */

    public List<String> getMorphology(List token_list) {
        Volume volume = null;
        volume = MappedFileVol.FACTORY.makeVolume(lang + ".db", true);

        SortedTableMap<String, String> map = SortedTableMap.open(volume, Serializer.STRING, Serializer.STRING);




        List<String> results = new LinkedList<String>();

        int threadsNumber = Runtime.getRuntime().availableProcessors();
        List<List<String>> parts;


        parts = Lists.partition(token_list, (token_list.size() / threadsNumber) + 1);

        try {
            executor = Executors.newFixedThreadPool(parts.size());
        } catch (Exception e) {
            e.printStackTrace();
        }

        callables = new LinkedHashSet<Callable<List<String>>>();

        for (int pts = 0; pts < parts.size(); pts++) {
            callables.add(new DigiMorph_Analizer(parts.get(pts), this.lang,map));
        }

        try {

            futures = executor.invokeAll(callables);
            executor.shutdown();
            executor.awaitTermination(Integer.MAX_VALUE, TimeUnit.SECONDS);

            executor.shutdownNow();
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            for (int i = 0; i < futures.size(); i++) {
                results.addAll(futures.get(i).get());

            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return results;
    }


    Map<String, String> mapcodgram = new HashMap<String, String>();
    Map<String, String> mapcodfless = new HashMap<String, String>();




    public void re_train(String csv_path) {
        File dbf = new File(lang + ".db");
        if (dbf.exists()) {
            dbf.delete();
        }
        fill_codgram();
        fill_codfless();
        Volume volume = MappedFileVol.FACTORY.makeVolume(lang + ".db", false);
        SortedTableMap.Sink<String, String> sink =
                SortedTableMap.create(
                        volume,
                        Serializer.STRING, // key serializer
                        Serializer.STRING   // value serializer
                )
                        .pageSize(64 * 1024)
                        .nodeSize(8)
                        .createFromSink();


        SortedMap<String, String> map = new TreeMap<String, String>();
        try {
            Reader in = new FileReader(csv_path);
            Iterable<CSVRecord> records = CSVFormat.EXCEL.parse(in);
            for (CSVRecord record : records) {
                String codgram = record.get(0);
                String lemma = StringEscapeUtils.unescapeHtml4(record.get(1));
                String codflessione = record.get(2);
                String forma = StringEscapeUtils.unescapeHtml4(record.get(3));
                if (!map.containsKey(forma)) {
                    map.put(forma, "");
                }
                if (lemma == null) {
                    lemma = "";
                }
                if (codflessione == null) {
                    codflessione = "";
                }
                String newcode = mapcodfless.get(codflessione);

                if (newcode != null) {
                    newcode = newcode.replace("•", lemma + "+"+mapcodgram.get(codgram)+"+");
                } else {
                    newcode = "nil";
                }

                map.put(forma, map.get(forma) + " " + lemma + "+" + mapcodgram.get(codgram) + "+" + newcode);

            }


            for (Map.Entry<String, String> e : map.entrySet()) {
                sink.put(e.getKey(), e.getValue());
            }


        } catch (Exception e) {
            e.printStackTrace();
        }


        SortedTableMap<String, String> stmap = sink.create();
        volume.close();

        System.out.println("done");


    }




    private void fill_codfless() {

    }


    private void fill_codgram() {

    }


}
