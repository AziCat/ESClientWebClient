package cn.sinobest.simplequery.domain;

import java.util.Map;

public class Source {
    private String _index;
    private String _type;
    private String _id;
    private float _score;
    private Map item;

    public String get_index() {
        return _index;
    }

    public void set_index(String _index) {
        this._index = _index;
    }

    public String get_type() {
        return _type;
    }

    public void set_type(String _type) {
        this._type = _type;
    }

    public String get_id() {
        return _id;
    }

    public void set_id(String _id) {
        this._id = _id;
    }

    public float get_score() {
        return _score;
    }

    public void set_score(float _score) {
        this._score = _score;
    }

    public Map getItem() {
        return item;
    }

    public void setItem(Map item) {
        this.item = item;
    }
}
