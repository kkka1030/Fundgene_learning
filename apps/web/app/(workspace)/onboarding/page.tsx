import { OnboardingWorkflow } from "@/components/onboarding-workflow";
import { SectionBlock } from "@/components/section-block";

export default function OnboardingPage() {
  return (
    <div className="space-y-8">
      <SectionBlock
        eyebrow="建档"
        title="先建立可解释的用户画像，再进入日常工作台决策流。"
        description="基础资料和风险问卷会决定工作台、教练和训练的初始方向。"
      >
        <OnboardingWorkflow />
      </SectionBlock>
    </div>
  );
}
