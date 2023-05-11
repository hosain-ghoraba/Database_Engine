package M2;

import M1.DBAppException;

import java.util.Vector;

public class CheckerOfString {

    private Vector<String> words;
    private int wordIndex;

    public CheckerOfString(String str){
        words = new Vector<>();
        boolean isVarchar = false;
        boolean isChar = false;
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < str.length(); i++) {
            if (str.charAt(i) == '\'' && isVarchar) {
                sb.append('\'');
                words.add(sb.toString());
                isVarchar = false;
                sb = new StringBuilder();
            } else if (!isVarchar && str.charAt(i) == '\'') {
                sb.append('\'');
                isVarchar = true;
            } else if (isVarchar) {
                sb.append(str.charAt(i));
            } else if (isChar && isDelimiter(str.charAt(i))) {
                words.add(sb.toString());
                isChar = false;
                sb = new StringBuilder();
                if (isDelimiter2(str.charAt(i)))
                    words.add(str.charAt(i) + "");
            } else if (!isDelimiter(str.charAt(i)) && !isOperator(str.charAt(i))) {
                sb.append(str.charAt(i));
                isChar = true;
            } else if (isDelimiter2(str.charAt(i))) {
                words.add(str.charAt(i) + "");
            } else if (isChar && isOperator(str.charAt(i))) {
                words.add(sb.toString());
                isChar = false;
                sb = new StringBuilder();
                if (isOperator(str.charAt(i + 1)))
                    words.add(str.charAt(i++) + "" + str.charAt(i));
                else
                    words.add(str.charAt(i) + "");
            } else if (!isChar && isOperator(str.charAt(i))) {
                isChar = false;
                sb = new StringBuilder();
                if (isOperator(str.charAt(i + 1)))
                    words.add(str.charAt(i++) + "" + str.charAt(i));
                else
                    words.add(str.charAt(i) + "");
            }
        }


    }

    private static boolean isOperator(char c) {
        return c == '>' || c == '<' || c == '=' || c == '!';
    }

    private static boolean isDelimiter(char c) {
        return c == ' ' || c == ',' || c == '(' || c == ')' || c == '\n' || c == '\r';
    }

    private static boolean isDelimiter2(char c) {
        return c == ',' || c == '(' || c == ')';
    }
    public String nextWord() throws DBAppException {
        if (wordIndex == words.size())
            throw new DBAppException("invalid SQL program");
        if (words.get(wordIndex).charAt(0) != '\'')
            return words.get(wordIndex++).toLowerCase();
        else
            return words.get(wordIndex++);
    }

    public String readNextWord() throws DBAppException {
        if (wordIndex == words.size())
            throw new DBAppException("invalid SQL program");
        if (words.get(wordIndex).charAt(0) != '\'')
            return words.get(wordIndex).toLowerCase();
        else
            return words.get(wordIndex);
    }
    public boolean hasMoreWords() {
        return !(wordIndex == words.size());
    }

}
