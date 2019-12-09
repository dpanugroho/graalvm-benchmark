package org.bdapro.tpch;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

class Queries {
    public static double tpch6(List<LineItem> items) {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd");
        Date startDate = null;
        Date endDate = null;
        try {
            startDate = simpleDateFormat.parse("19940101");
            endDate = simpleDateFormat.parse("19950101");
        } catch (ParseException e) {
            e.printStackTrace();
        }

        Date finalStartDate = startDate;
        Date finalEndDate = endDate;
        return items.stream().filter(lineItem -> lineItem.getlShipDate().compareTo(finalStartDate) >= 0 &&
                lineItem.getlShipDate().compareTo(finalEndDate) < 0 && lineItem.getlDiscount() > (0.06 - 0.01) &&
                lineItem.getlDiscount() < (0.06 + 0.01) && lineItem.getlQuantity() < 24)
                .mapToDouble(value -> value.getlExtendedPrice() * value.getlDiscount()).sum();
    }
}
