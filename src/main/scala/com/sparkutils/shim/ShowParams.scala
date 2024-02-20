package com.sparkutils.shim

/**
 * Paramters to pass into showString for debugging / validation
 * @param numRows defaults to 1000
 * @param truncate
 * @param vertical
 */
case class ShowParams(numRows: Int = 1000, truncate: Int = 0, vertical: Boolean = false)
