package edu.cmu.ml.rtw.users.matt.randomwalks;

import java.util.ArrayList;
import java.util.List;

public class WalkPath {
    private List<String> items;
    private List<Boolean> is_node;

    public WalkPath() {
        items = new ArrayList<String>();
        is_node = new ArrayList<Boolean>();
    }

    public void addRelation(String relation) {
        items.add(relation);
        is_node.add(false);
    }

    public void addNode(String node) {
        items.add(node);
        is_node.add(true);
    }

    public String getPathString(boolean remove_cycles, boolean lexicalize_paths) {
        List<String> reverse_items = new ArrayList<String>();
        List<Boolean> reverse_is_node = new ArrayList<Boolean>();
        if (remove_cycles) {
            for (int i=items.size()-1; i>=0; i--) {
                reverse_items.add(items.get(i));
                reverse_is_node.add(is_node.get(i));
                if (!is_node.get(i)) continue;
                for (int j=i; j>=0; j--) {
                    if (!is_node.get(j)) continue;
                    if (items.get(i).equals(items.get(j))) {
                        i = j;
                    }
                }
            }
        }
        String path_str = "-";
        for (int i=reverse_items.size()-1; i>=0; i--) {
            if (!reverse_is_node.get(i) || lexicalize_paths) {
                path_str += reverse_items.get(i) + "-";
            }
        }
        return path_str;
    }
}
