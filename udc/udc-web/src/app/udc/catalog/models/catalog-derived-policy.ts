import {DerivedPolicyItem} from './catalog-derived-policy-item';

export class DerivedPolicy {
  public derivedPolicyId: number;
  public policyId: number;
  public policyName: string;
  public typeName: string;
  public policyLocations: string;
  public clusterId: number;
  public policyItems = new Array<DerivedPolicyItem>();
}
