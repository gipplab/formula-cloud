package mir.formulacloud;

import mir.formulacloud.beans.Document;
import org.junit.jupiter.api.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author Andre Greiner-Petter
 */
public class DocumentTests {
    private static final String testString = "<mrow xmlns:mws=\"http://search.mathweb.org/ns\">\n" +
            "    <mrow>\n" +
            "      <mo>|</mo>\n" +
            "      <mrow>\n" +
            "        <mo>{</mo>\n" +
            "        <mrow>\n" +
            "          <mi>μ</mi>\n" +
            "          <mo>∈</mo>\n" +
            "          <mi>M</mi>\n" +
            "        </mrow>\n" +
            "        <mo>∣</mo>\n" +
            "        <mrow>\n" +
            "          <mrow>\n" +
            "            <msub>\n" +
            "              <mi>\uD835\uDD09</mi>\n" +
            "              <mi>μ</mi>\n" +
            "            </msub>\n" +
            "            <mo>∩</mo>\n" +
            "            <mi>C</mi>\n" +
            "          </mrow>\n" +
            "          <mo>=</mo>\n" +
            "          <mi mathvariant=\"normal\">∅</mi>\n" +
            "        </mrow>\n" +
            "        <mo>}</mo>\n" +
            "      </mrow>\n" +
            "      <mo>|</mo>\n" +
            "    </mrow>\n" +
            "    <mo>≥</mo>\n" +
            "    <mi>n</mi>\n" +
            "  </mrow>";

    private static final String result = "<mrow><mrow><mo>|</mo>" +
            "<mrow><mo>{</mo><mrow><mi>μ</mi><mo>∈</mo><mi>M</mi></mrow>" +
            "<mo>∣</mo><mrow><mrow><msub><mi>\uD835\uDD09</mi><mi>μ</mi></msub>" +
            "<mo>∩</mo><mi>C</mi></mrow><mo>=</mo><mi>∅</mi></mrow><mo>}</mo>" +
            "</mrow><mo>|</mo></mrow><mo>≥</mo><mi>n</mi></mrow>";

    @Test
    public void testCleanString() {
        assertEquals(result, Document.cleanExpression(testString));
    }
}
