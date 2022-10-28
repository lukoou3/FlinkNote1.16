package com.flink.utils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class ArgsTool {
    protected final Map<String, String> kwargs;
    protected final String[] args;

    private ArgsTool(Map<String, String> kwargs, String[] args) {
        this.kwargs = kwargs;
        this.args = args;
    }

    /**
     * Keys have to start with '-' or '--'
     * Example arguments:
     * --key1 value1 --key2 value2 -key3 value3 value4 value5
     * @param args
     * @return
     */
    public static ArgsTool fromArgs(String[] args) {
        final Map<String, String> map = new HashMap<>(args.length);

        int i = 0;
        while (i < args.length) {
            String key = null;
            if (args[i].startsWith("--")) {
                key = args[i].substring(2);
            } else if (args[i].startsWith("-")) {
                key = args[i].substring(1);
            }

            if (key == null || key.isEmpty()) {
                break;
            }

            i += 1; // next value index

            if (i >= args.length) {
                map.put(key, "");
            }  else if (args[i].startsWith("--") || args[i].startsWith("-")) {
                map.put(key, "");
            } else {
                map.put(key, args[i]);
                i += 1;
            }
        }

        String[] pargs = null;
        if(i >= args.length){
            pargs = new String[0];
        }else {
            pargs = new String[args.length - i];
            System.arraycopy(args, i, pargs, 0, args.length - i);
        }

        return new ArgsTool(map, pargs);
    }

    public String get(String key) {
        return kwargs.get(key);
    }

    public String get(String key, String defaultValue) {
        return kwargs.getOrDefault(key,defaultValue);
    }

    public int getInt(String key, int defaultValue) {
        String value = get(key);
        if (value == null) {
            return defaultValue;
        }
        return Integer.parseInt(value);
    }

    public long getLong(String key, int defaultValue) {
        String value = get(key);
        if (value == null) {
            return defaultValue;
        }
        return Long.parseLong(value);
    }

    public Map<String, String> getKwargs() {
        return kwargs;
    }

    public String[] getArgs() {
        return args;
    }

    public static void main(String[] args) {
        System.out.println(Arrays.asList(args));
        System.out.println(args.length);

        ArgsTool argsTool = ArgsTool.fromArgs(args);
        System.out.println(argsTool.getKwargs());
        System.out.println(Arrays.asList(argsTool.getArgs()));
    }

}
