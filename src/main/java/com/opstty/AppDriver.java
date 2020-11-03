package com.opstty;

import com.opstty.job.Dt;
import com.opstty.job.Species;
import com.opstty.job.TreeBySpecies;
import com.opstty.job.WordCount;
import org.apache.hadoop.util.ProgramDriver;

import java.util.function.DoubleToIntFunction;

public class AppDriver {
    public static void main(String argv[]) {
        int exitCode = -1;
        ProgramDriver programDriver = new ProgramDriver();

        try {
            programDriver.addClass("highest", Species.class,
                    "A map/reduce program that return the high of the highest tree of each species.");
            programDriver.addClass("species", Species.class,
                    "A map/reduce program that list the species of trees.");
            programDriver.addClass("tbs", TreeBySpecies.class,
                    "A map/reduce program that counts the number of tree by species.");
            programDriver.addClass("wordcount", WordCount.class,
                    "A map/reduce program that counts the words in the input files.");
            programDriver.addClass("dt", Dt.class,
                    "A map/reduce program that counts the number of district with trees.");

            exitCode = programDriver.run(argv);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }


        System.exit(exitCode);
    }
}
