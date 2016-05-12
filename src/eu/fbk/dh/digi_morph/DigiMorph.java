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




        List<String> results = new LinkedList<>();

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


    Map<String, String> mapcodgram = new HashMap<>();
    Map<String, String> mapcodfless = new HashMap<>();





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
        mapcodfless.put("f", "f");
        mapcodfless.put("fn", "f+_");
        mapcodfless.put("fp", "f+plur");
        mapcodfless.put("fp0", "f+plur");
        mapcodfless.put("fp1", "f+plur");
        mapcodfless.put("fp1s", "f+plur");
        mapcodfless.put("fp2", "f+plur");
        mapcodfless.put("fp3", "f+plur");
        mapcodfless.put("fpa", "f+plur");
        mapcodfless.put("fpc", "f+plur");
        mapcodfless.put("fppr", "f+plur+part+pass");
        mapcodfless.put("fps", "f+plur+sup");
        mapcodfless.put("fs", "f+sing");
        mapcodfless.put("fs0", "f+sing");
        mapcodfless.put("fs1", "f+sing");
        mapcodfless.put("fs1s", "f+sing");
        mapcodfless.put("fs2", "f+sing");
        mapcodfless.put("fs3", "f+sing");
        mapcodfless.put("fsa", "f+sing");
        mapcodfless.put("fsc", "f+sing");
        mapcodfless.put("fspr", "f+sing+part+pass");
        mapcodfless.put("fss", "f+sing+sup");
        mapcodfless.put("g", "gerundio");
        mapcodfless.put("m", "m");
        mapcodfless.put("mm", "m");
        mapcodfless.put("mn", "m+_");
        mapcodfless.put("mnpr", "m+part+pass");
        mapcodfless.put("mp", "m+plur");
        mapcodfless.put("mp0", "m+plur");
        mapcodfless.put("mp1", "m+plur");
        mapcodfless.put("mp1s", "m+plur");
        mapcodfless.put("mp2", "m+plur");
        mapcodfless.put("mp3", "m+plur");
        mapcodfless.put("mpa", "m+plur");
        mapcodfless.put("mpc", "m+plur");
        mapcodfless.put("mppr", "m+plur+part+pass");
        mapcodfless.put("mps", "m+plur+sup");
        mapcodfless.put("ms", "m+sing");
        mapcodfless.put("ms0", "m+sing");
        mapcodfless.put("ms1", "m+sing");
        mapcodfless.put("ms1s", "m+sing");
        mapcodfless.put("ms2", "m+sing");
        mapcodfless.put("ms3", "m+sing");
        mapcodfless.put("msa", "m+sing");
        mapcodfless.put("msc", "m+sing");
        mapcodfless.put("mspr", "m+sing+part+pass");
        mapcodfless.put("mss", "m+sing+sup");
        mapcodfless.put("n", "_");
        mapcodfless.put("nn", "_");
        mapcodfless.put("nn0", "_");
        mapcodfless.put("nn3", "_");
        mapcodfless.put("nnpp", "_+part+pass");
        mapcodfless.put("nns", "_+sup");
        mapcodfless.put("np", "_+plur");
        mapcodfless.put("np0", "_+plur");
        mapcodfless.put("np1", "_+plur");
        mapcodfless.put("np2", "_+plur");
        mapcodfless.put("np3", "_+plur");
        mapcodfless.put("npc", "_+plur+comp");
        mapcodfless.put("nppp", "+plur+part+pres");
        mapcodfless.put("ns", "_+sing");
        mapcodfless.put("ns0", "_+sing");
        mapcodfless.put("ns1", "_+sing");
        mapcodfless.put("ns2", "_+sing");
        mapcodfless.put("ns3", "_+sing");
        mapcodfless.put("nsc", "_+sing+comp");
        mapcodfless.put("nspp", "_+sing+part+pres");
        mapcodfless.put("p", "plur");
        mapcodfless.put("p1ci", "cong+impf+1+plur");
        mapcodfless.put("p1cp", "cong+pres+1+plur");
        mapcodfless.put("p1cpp1ip", "cong+pres+1+plur •indic+pres+1+plur");
        mapcodfless.put("p1dp", "cond+pres+1+plur");
        mapcodfless.put("p1if", "indic+fut+1+plur");
        mapcodfless.put("p1ii", "indic+imperf+1+plur");
        mapcodfless.put("p1ip", "indic+pres+1+plur");
        mapcodfless.put("p1ipp1ci", "indic+pres+1+plur •cong+pres+1+plur");
        mapcodfless.put("p1ipp1cp", "indic+pres+1+plur •cong+pres+1+plur");
        mapcodfless.put("p1ir", "indic+pass+rem+1+plur");
        mapcodfless.put("p2ci", "cong+impf+2+plur");
        mapcodfless.put("p2cp", "cond+pres+2+plur");
        mapcodfless.put("p2cpp2mp", "cong+pres+2+plur •impr+_+2+plur");
        mapcodfless.put("p2dp", "cond+pres+2+plur");
        mapcodfless.put("p2if", "indic+fut+2+plur");
        mapcodfless.put("p2ii", "indic+imperf+2+plur");
        mapcodfless.put("p2ip", "indic+pres+2+plur");
        mapcodfless.put("p2ipp2mp", "indic+pres+2+plur •impr+_+2+plur");
        mapcodfless.put("p2ips2mp", "indic+pres+2+plur •impr+_+2+plur");
        mapcodfless.put("p2ir", "indic+pass+rem+2+plur");
        mapcodfless.put("p2irp2ci", "indic+pass+rem+2+plur •cong+imperf+2+plur");
        mapcodfless.put("p3ci", "cong+imperf+3+plur");
        mapcodfless.put("p3cp", "cong+pres+3+plur");
        mapcodfless.put("p3dp", "cond+pres+3+plur");
        mapcodfless.put("p3if", "indic+fut+3+plur");
        mapcodfless.put("p3ii", "indic+imperf+3+plur");
        mapcodfless.put("p3ip", "indic+pres+3+plur");
        mapcodfless.put("p3ir", "indic+pass+rem+3+plur");
        mapcodfless.put("s", "sup");
        mapcodfless.put("s1cis2ci", "cong+imperf+1+sing •cong+imperf+2+sing");
        mapcodfless.put("s1cp", "cong+pres+1+sing");
        mapcodfless.put("s1cps2cp", "cong+pres+1+sing •cong+pres+2+sing");
        mapcodfless.put("s1cps2cps3cp", "cong+pres+1+sing •cong+pres+2+sing •cong+pres+3+sing");
        mapcodfless.put("s1cps2cps3", "cong+pres+1+sing •cong+pres+2+sing •cong+pres+3+sing");
        mapcodfless.put("s1cps2cps3cps2mp", "cong+pres+1+sing •cong+pres+2+sing •cong+pres+3+sing •impr+_+2+sing");
        mapcodfless.put("s1dp", "cond+pres+1+sing");
        mapcodfless.put("s1dps3dp", "cond+pres+1+sing •cond+pres+3+sing");
        mapcodfless.put("s1if", "indic+fut+1+sing");
        mapcodfless.put("s1ii", "indic+imperf+1+sing");
        mapcodfless.put("s1ip", "indic+pres+1+sing");
        mapcodfless.put("s1ipp3ip", "indic+pres+1+sing •indic+pres+3+plur");
        mapcodfless.put("s1ir", "indic+pass+rem+1+sing");
        mapcodfless.put("s2cp", "cong+pres+2+sing");
        mapcodfless.put("s2dp", "cond+pres+2+sing");
        mapcodfless.put("s2if", "indic+fut+2+sing");
        mapcodfless.put("s2ii", "indic+imperf+2+sing");
        mapcodfless.put("s2ip", "indic+pres+2+sing");
        mapcodfless.put("s2ips1cps2cps3cp", "indic+pres+2+sing •cong+pres+1+sing •cong+pres+2+sing •cong+pres+3+sing");
        mapcodfless.put("s2ips1cps2", "indic+pres+2+sing •cong+pres+1+sing •cong+pres+2+sing •cong+pres+3+sing");
        mapcodfless.put("s2ips2mp", "indic+pres+2+sing •impr+2+sing");
        mapcodfless.put("s2ir", "indic+pass+rem+2+sing");
        mapcodfless.put("s2mp", "impr+2+sing");
        mapcodfless.put("s2mps2cp", "impr+2+sing •cong+pres+2+sing");
        mapcodfless.put("s2mps3ip", "impr+2+sing •indic+pres+2+sing");
        mapcodfless.put("s3ci", "cong+imperf+3+sing");
        mapcodfless.put("s3cp", "cong+imperf+3+sing");
        mapcodfless.put("s3dp", "cond+pres+3+sing");
        mapcodfless.put("s3if", "indic+fut+3+sing");
        mapcodfless.put("s3ii", "indic+imperf+3+sing");
        mapcodfless.put("s3ip", "indic+pres+3+sing");
        mapcodfless.put("s3ips2mp", "indic+pres+3+sing •impr+2+sing");
        mapcodfless.put("s3ir", "indic+pass+rem+3+sing");
        mapcodfless.put("siif", "impr");
        mapcodfless.put("sp", "impr");
        mapcodfless.put("spr", "impr+part+pass+_+sing");
        mapcodfless.put("inf", "infinito");
    }


    private void fill_codgram() {
        mapcodgram.put("a", "adj");
        mapcodgram.put("af", "adj");
        mapcodgram.put("afl", "adj");
        mapcodgram.put("al", "adj");
        mapcodgram.put("am", "adj");
        mapcodgram.put("aml", "adj");
        mapcodgram.put("an", "adj");
        mapcodgram.put("as", "adj");
        mapcodgram.put("b", "adv+modal");
        mapcodgram.put("bs", "adv");
        mapcodgram.put("c", "conj+sub");
        mapcodgram.put("cc", "conj+coo");
        mapcodgram.put("d", "adj+pron");
        mapcodgram.put("dd", "adj+dim");
        mapcodgram.put("de", "adj+excl");
        mapcodgram.put("di", "adj+ind");
        mapcodgram.put("dil", "adj+ind");
        mapcodgram.put("dn", "adj+num");
        mapcodgram.put("dp", "adj+poss");
        mapcodgram.put("dr", "adj+rel");
        mapcodgram.put("dt", "adj+int");
        mapcodgram.put("e", "prep");
        mapcodgram.put("e0", "prep");
        mapcodgram.put("i", "int");
        mapcodgram.put("i0", "int");
        mapcodgram.put("l", "loc");
        mapcodgram.put("la", "loc_adv");
        mapcodgram.put("lc", "loc_cong");
        mapcodgram.put("lp", "loc_prep");
        mapcodgram.put("n", "num");
        mapcodgram.put("pc", "pron+rec");
        mapcodgram.put("pd", "pron+dim");
        mapcodgram.put("pe", "pron+excl");
        mapcodgram.put("pf", "pron+rifl");
        mapcodgram.put("pi", "pron+ind");
        mapcodgram.put("pn", "pron+num");
        mapcodgram.put("pp", "pron+poss");
        mapcodgram.put("pq", "pron+pers");
        mapcodgram.put("pr", "pron+rel");
        mapcodgram.put("r", "art");
        mapcodgram.put("rd", "art+det");
        mapcodgram.put("ri", "art+indet");
        mapcodgram.put("sd", "n");
        mapcodgram.put("sf", "n");
        mapcodgram.put("sfl", "n");
        mapcodgram.put("sg", "n");
        mapcodgram.put("si", "n");
        mapcodgram.put("sm", "n");
        mapcodgram.put("sml", "n");
        mapcodgram.put("sn", "n");
        mapcodgram.put("v", "v");
        mapcodgram.put("va", "v");
        mapcodgram.put("vb", "v");
        mapcodgram.put("vc", "v");
        mapcodgram.put("vd", "v");
        mapcodgram.put("ve", "v");
        mapcodgram.put("vei", "v");
        mapcodgram.put("veir", "v");
        mapcodgram.put("veit", "v");
        mapcodgram.put("vem", "v");
        mapcodgram.put("vet", "v");
        mapcodgram.put("vetb", "v");
        mapcodgram.put("vey", "v");
        mapcodgram.put("veyt", "v");
        mapcodgram.put("vf", "v");
        mapcodgram.put("vh", "v");
        mapcodgram.put("vhi", "v");
        mapcodgram.put("vht", "v");
        mapcodgram.put("vi", "v");
        mapcodgram.put("via", "v");
        mapcodgram.put("vib", "v");
        mapcodgram.put("vie", "v");
        mapcodgram.put("viet", "v");
        mapcodgram.put("vih", "v");
        mapcodgram.put("vin", "v");
        mapcodgram.put("vip", "v");
        mapcodgram.put("vipbt", "v");
        mapcodgram.put("vipt", "v");
        mapcodgram.put("vir", "v");
        mapcodgram.put("virp", "v");
        mapcodgram.put("vit", "v");
        mapcodgram.put("vitb", "v");
        mapcodgram.put("vitbe", "v");
        mapcodgram.put("vitp", "v");
        mapcodgram.put("vitr", "v");
        mapcodgram.put("vity", "v");
        mapcodgram.put("viy", "v");
        mapcodgram.put("viyp", "v");
        mapcodgram.put("viypt", "v");
        mapcodgram.put("viyt", "v");
        mapcodgram.put("viyte", "v");
        mapcodgram.put("vl", "v");
        mapcodgram.put("vli", "v");
        mapcodgram.put("vlp", "v");
        mapcodgram.put("vlr", "v");
        mapcodgram.put("vm", "v");
        mapcodgram.put("vn", "v");
        mapcodgram.put("vnb", "v");
        mapcodgram.put("vne", "v");
        mapcodgram.put("vni", "v");
        mapcodgram.put("vnp", "v");
        mapcodgram.put("vnpb", "v");
        mapcodgram.put("vnr", "v");
        mapcodgram.put("vnt", "v");
        mapcodgram.put("vntbr", "v");
        mapcodgram.put("vntibe", "v");
        mapcodgram.put("vntip", "v");
        mapcodgram.put("vny", "v");
        mapcodgram.put("vp", "v");
        mapcodgram.put("vpb", "v");
        mapcodgram.put("vpi", "v");
        mapcodgram.put("vpt", "v");
        mapcodgram.put("vr", "v");
        mapcodgram.put("vrp", "v");
        mapcodgram.put("vt", "v");
        mapcodgram.put("vta", "v");
        mapcodgram.put("vtae", "v");
        mapcodgram.put("vtai", "v");
        mapcodgram.put("vtaip", "v");
        mapcodgram.put("vtap", "v");
        mapcodgram.put("vtb", "v");
        mapcodgram.put("vtbp", "v");
        mapcodgram.put("vtbpi", "v");
        mapcodgram.put("vtbpr", "v");
        mapcodgram.put("vtbr", "v");
        mapcodgram.put("vtbri", "v");
        mapcodgram.put("vtc", "v");
        mapcodgram.put("vtd", "v");
        mapcodgram.put("vtdi", "v");
        mapcodgram.put("vte", "v");
        mapcodgram.put("vteb", "v");
        mapcodgram.put("vtei", "v");
        mapcodgram.put("vteipr", "v");
        mapcodgram.put("vtep", "v");
        mapcodgram.put("vter", "v");
        mapcodgram.put("vterp", "v");
        mapcodgram.put("vtey", "v");
        mapcodgram.put("vth", "v");
        mapcodgram.put("vti", "v");
        mapcodgram.put("vtia", "v");
        mapcodgram.put("vtiab", "v");
        mapcodgram.put("vtib", "v");
        mapcodgram.put("vtibp", "v");
        mapcodgram.put("vtid", "v");
        mapcodgram.put("vtie", "v");
        mapcodgram.put("vtier", "v");
        mapcodgram.put("vtin", "v");
        mapcodgram.put("vtip", "v");
        mapcodgram.put("vtipa", "v");
        mapcodgram.put("vtipb", "v");
        mapcodgram.put("vtipr", "v");
        mapcodgram.put("vtir", "v");
        mapcodgram.put("vtirb", "v");
        mapcodgram.put("vtire", "v");
        mapcodgram.put("vtirey", "v");
        mapcodgram.put("vtirp", "v");
        mapcodgram.put("vtirpy", "v");
        mapcodgram.put("vtiy", "v");
        mapcodgram.put("vtiyr", "v");
        mapcodgram.put("vtiyrp", "v");
        mapcodgram.put("vtl", "v");
        mapcodgram.put("vtm", "v");
        mapcodgram.put("vtmp", "v");
        mapcodgram.put("vtn", "v");
        mapcodgram.put("vtni", "v");
        mapcodgram.put("vtnp", "v");
        mapcodgram.put("vtnr", "v");
        mapcodgram.put("vtnre", "v");
        mapcodgram.put("vtp", "v");
        mapcodgram.put("vtpb", "v");
        mapcodgram.put("vtpbr", "v");
        mapcodgram.put("vtpe", "v");
        mapcodgram.put("vtpi", "v");
        mapcodgram.put("vtpr", "v");
        mapcodgram.put("vtpri", "v");
        mapcodgram.put("vtpy", "v");
        mapcodgram.put("vtr", "v");
        mapcodgram.put("vtra", "v");
        mapcodgram.put("vtrb", "v");
        mapcodgram.put("vtrbi", "v");
        mapcodgram.put("vtrbp", "v");
        mapcodgram.put("vtre", "v");
        mapcodgram.put("vtrei", "v");
        mapcodgram.put("vtrep", "v");
        mapcodgram.put("vtri", "v");
        mapcodgram.put("vtrip", "v");
        mapcodgram.put("vtrn", "v");
        mapcodgram.put("vtrp", "v");
        mapcodgram.put("vtrpb", "v");
        mapcodgram.put("vtrpi", "v");
        mapcodgram.put("vtrpiy", "v");
        mapcodgram.put("vtrpy", "v");
        mapcodgram.put("vty", "v");
        mapcodgram.put("vtyip", "v");
        mapcodgram.put("vy", "v");
        mapcodgram.put("vyei", "v");
        mapcodgram.put("vyi", "v");
        mapcodgram.put("vyit", "v");
        mapcodgram.put("vyt", "v");
        mapcodgram.put("vyte", "v");
    }


}
