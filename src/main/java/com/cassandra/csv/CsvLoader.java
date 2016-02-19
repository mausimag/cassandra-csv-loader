package com.cassandra.csv;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Created by mauricio on 18/02/2016.
 */
public class CsvLoader
{
    private Path filePath;
    private Charset charset = Charset.forName("UTF-8");
    private BufferedReader reader;

    public CsvLoader()
    {
    }

    public void open(String path)
    {
        filePath = Paths.get(path);
    }

    public void forEachLine(Line val)
    {
        long counter = 0;
        String line;
        try (BufferedReader reader = Files.newBufferedReader(filePath, charset))
        {
            while ((line = reader.readLine()) != null)
            {
                counter++;
                System.out.println("Processed: " + counter + " lines");

                val.line(line);
            }
        } catch (IOException e)
        {
            System.err.println(e);
        }
    }

    public void forEachLine(String delimiter, ParsedLine val)
    {
        String line;
        try (BufferedReader reader = Files.newBufferedReader(filePath, charset))
        {
            while ((line = reader.readLine()) != null)
            {
                val.line(line.split(delimiter));
            }
        } catch (IOException e)
        {
            System.err.println(e);
        }
    }
}
