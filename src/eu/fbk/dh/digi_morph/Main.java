package eu.fbk.dh.digi_morph;

import org.apache.commons.cli.*;

import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;

public class Main {

    private static void printUsage(Options opt){
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("DigiMorph", opt);
        System.exit(1);
    }


    private static void retrain(String filepath){
        DigiMorph dm = new DigiMorph("italian");
        dm.re_train(filepath);
        System.exit(0);
    }

    public static void main(String[] args) {
        Options options = new Options();
        options.addOption(Option.builder("r").hasArg().argName("path to file").desc("Retrain Morphological Anlizer").build());
        options.addOption("version","print the tool version");
        CommandLineParser parser = new DefaultParser();

        try {
            CommandLine cmd = parser.parse(options, args);
            if (cmd.hasOption("--version")){
                System.out.println(DigiMorph.getVersion());
                System.exit(0);

            }

            if (cmd.hasOption('r')){
                if(cmd.getOptionValue('r') != null) {
                    retrain(cmd.getOptionValue('r'));
                }else {
                    printUsage(options);
                }
            }


        } catch (Exception e) {
           printUsage(options);
        }


        DigiMorph dm = new DigiMorph("italian");

        List<String> text = new LinkedList<>();


        Scanner scanner = new Scanner(System.in);
        while (scanner.hasNextLine()) {
            text.add(scanner.nextLine());

        }
        for (String s : dm.getMorphology(text)) {
            System.out.println(s);
        }


    }
}

