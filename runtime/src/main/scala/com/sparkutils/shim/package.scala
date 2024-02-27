package com.sparkutils

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

/**
 * A collection of functions with possibly varying behaviour across Spark versions.  Should actual implementations fracture they will be implemented as part of ShimUtils but the interface will remain to proxy the calls.
 */
package object shim {

  /**
   * Registers a session only plan via experimental methods when isPresentFilter is not true
   * @param logicalPlan
   * @param isPresentFilter a filter that should return true when the plan is identical and it should not be added
   * @return true if the plan has been added
   */
  def registerSessionPlan(logicalPlan: Rule[LogicalPlan])(isPresentFilter: Rule[LogicalPlan] => Boolean): Boolean = {
    val methods = SparkSession.active.sessionState.experimentalMethods
    if (methods.extraOptimizations.forall(!isPresentFilter(_))) {
      methods.extraOptimizations = methods.extraOptimizations :+ logicalPlan
      true
    } else
      false
  }

}

