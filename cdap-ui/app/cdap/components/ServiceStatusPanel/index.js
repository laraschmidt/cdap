/*
 * Copyright © 2016 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

import React, {PropTypes} from 'react';
require('./ServiceStatusPanel.less');
import ServiceStatus from '../ServiceStatus/index.js';

export default function ServiceStatusPanel({services, isLoading}) {

  var serviceList = services.map(function(service){
    return <ServiceStatus isLoading={isLoading} status={service.status} name={service.name}/>;
  });

  return (
    <div className="service-status-panel">
      {serviceList}
    </div>
  );
}

ServiceStatusPanel.propTypes = {
  services: PropTypes.array,
  isLoading: PropTypes.bool
};
