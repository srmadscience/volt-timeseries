/*
 * Copyright (C) 2025 Volt Active Data Inc.
 *
 * Use of this source code is governed by an MIT
 * license that can be found in the LICENSE file or at
 * https://opensource.org/licenses/MIT.
 */
package ie.voltdb.timeseries;

@SuppressWarnings("serial")
public class BigDecimalHasWrongScaleException extends Exception {

    public BigDecimalHasWrongScaleException() {
        super();
    }

    public BigDecimalHasWrongScaleException(String message) {
        super(message);
    }

    public BigDecimalHasWrongScaleException(Throwable cause) {
        super(cause);
     }

    public BigDecimalHasWrongScaleException(String message, Throwable cause) {
        super(message, cause);
    }

    public BigDecimalHasWrongScaleException(String message, Throwable cause, boolean enableSuppression,
            boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

}
