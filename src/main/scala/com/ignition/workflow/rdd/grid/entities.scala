package com.ignition.workflow.rdd.grid

import com.ignition.data._

/**
 * Returns input and output metadata of the step.
 */
trait MetaDataHolder {
  def outMetaData: Option[RowMetaData]
}