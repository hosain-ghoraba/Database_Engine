package octree;

import java.awt.*;
import java.awt.Point;

public class Main {

    public static void main(String[] args) {
        Octree<String> tree = new Octree<>(0,0,0, 16, 16, 16);

//        for(int x = 0; x < 16; x++){
//            for(int y = 0; y < 16; y++){
//                for(int z = 0; z < 16; z++){
//                    tree.insert(x, y, z, "test " + x + y + z);
//                }
//            }
//        }

        tree.insert(4, 5, 5, "y");
        



    }
}
