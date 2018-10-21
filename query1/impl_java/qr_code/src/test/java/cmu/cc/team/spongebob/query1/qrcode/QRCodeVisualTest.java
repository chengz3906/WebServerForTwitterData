package cmu.cc.team.spongebob.query1.qrcode;

import cmu.cc.team.spongebob.query1.qrcode.utils.BinarySquare;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Test;

public class QRCodeVisualTest {
    @Test
    void ccTeamQRCode() {
        QRCodeParser encoder = new QRCodeParser();
        BinarySquare ccTeam = encoder.messageToBinarySquare("CC Team", false);

        printDivider("CC Team unencrypted");
        ccTeam.print();
    }

    private void printDivider(String text) {
        System.out.print(StringUtils.repeat('-', 5));
        System.out.print(text);
        System.out.print(StringUtils.repeat('-', 5));
        System.out.print('\n');
    }
}
