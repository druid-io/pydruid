Ensure {{ grains.cluster_name }} iam role exists:
  boto_iam_role.present:
    - name: {{ grains.cluster_name }}
    - create_instance_profile: False
    - policies_from_pillars:
       - orca_iam_development_policies
    - policy_document:
        Version: '2012-10-17'
        Statement:
          - Effect: Allow
            Principal:
              AWS: 'arn:aws:iam::{{pillar.aws_account_id.zimride}}:root'
            Action: 'sts:AssumeRole'
    - profile: orca_profile
