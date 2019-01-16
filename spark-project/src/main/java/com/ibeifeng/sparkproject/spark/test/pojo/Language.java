package com.ibeifeng.sparkproject.spark.test.pojo;

import java.io.Serializable;

public class Language implements Serializable{
    private String search_word;
    private String lang;
    private Integer hot_index;

    public Language() {
    }

    public Language(String search_word, String lang, Integer hot_index) {
        this.search_word = search_word;
        this.lang = lang;
        this.hot_index = hot_index;
    }

    public String getSearch_word() {
        return search_word;
    }

    public void setSearch_word(String search_word) {
        this.search_word = search_word;
    }

    public String getLang() {
        return lang;
    }

    public void setLang(String lang) {
        this.lang = lang;
    }

    public Integer getHot_index() {
        return hot_index;
    }

    public void setHot_index(Integer hot_index) {
        this.hot_index = hot_index;
    }

    @Override
    public String toString() {
        return "Language{" + "search_word='" + search_word + '\'' + ", lang='" + lang + '\'' + ", hot_index=" + hot_index + '}';
    }
}
