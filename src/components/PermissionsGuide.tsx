import {
  Accordion,
  AccordionContent,
  AccordionItem,
  AccordionTrigger,
} from "./ui/accordion";

const scopes = [
  {
    name: "files",
    description: "Read and write access to the File Manager",
    usedFor: "Media migration — download/upload files",
  },
  {
    name: "files.ui_hidden.read",
    description: "Read access to hidden files",
    usedFor: "Accessing files not visible in the UI",
  },
  {
    name: "content",
    description: "Read and write access to CMS content",
    usedFor: "Blog posts, pages, and email templates",
  },
  {
    name: "hubdb",
    description: "Read and write access to HubDB tables",
    usedFor: "HubDB table migration",
  },
];

const steps = [
  {
    number: 1,
    title: "Open your HubSpot portal settings",
    description:
      'Log in to HubSpot and click the gear icon in the top navigation bar, or go to Settings from the main menu.',
  },
  {
    number: 2,
    title: "Navigate to Account Management > Keys",
    description:
      'In the left sidebar, expand "Account Management", then expand "Keys".',
  },
  {
    number: 3,
    title: 'Click "Service Keys" (Beta)',
    description:
      'Under the Keys section, select "Service Keys". This is a newer feature currently in beta.',
  },
  {
    number: 4,
    title: "Create a new Service Key",
    description:
      'Click "Create Service Key". Give it a name (e.g. "MEMIT Migration") and configure the required scopes listed below.',
  },
  {
    number: 5,
    title: "Copy the access token",
    description:
      'After creating the key, copy the access token. This is what you paste into the Source or Target Access Token field above. Store it securely — you may not be able to view it again.',
  },
  {
    number: 6,
    title: "Repeat for the other portal",
    description:
      "You need a Service Key with the same scopes in both the source and target HubSpot portals.",
  },
];

export function PermissionsGuide() {
  return (
    <Accordion>
      <AccordionItem value="setup">
        <AccordionTrigger className="text-sm">
          How to get your Access Tokens
        </AccordionTrigger>
        <AccordionContent>
          <div className="space-y-4">
            <p className="text-sm text-muted-foreground">
              MEMIT connects to HubSpot using Service Key access tokens. Follow
              these steps for both your source and target portals:
            </p>

            <ol className="space-y-3">
              {steps.map((step) => (
                <li key={step.number} className="flex gap-3">
                  <span className="flex h-6 w-6 shrink-0 items-center justify-center rounded-full bg-primary text-xs font-semibold text-primary-foreground">
                    {step.number}
                  </span>
                  <div className="pt-0.5">
                    <p className="text-sm font-medium">{step.title}</p>
                    <p className="mt-0.5 text-xs text-muted-foreground">
                      {step.description}
                    </p>
                  </div>
                </li>
              ))}
            </ol>
          </div>
        </AccordionContent>
      </AccordionItem>

      <AccordionItem value="scopes">
        <AccordionTrigger className="text-sm">
          Required Service Key Scopes
        </AccordionTrigger>
        <AccordionContent>
          <p className="mb-3 text-sm text-muted-foreground">
            When creating your Service Key (Step 4 above), enable all of the
            following scopes:
          </p>
          <div className="space-y-2">
            {scopes.map((scope) => (
              <div
                key={scope.name}
                className="rounded-md border bg-muted/30 px-3 py-2"
              >
                <div className="flex items-baseline justify-between">
                  <code className="text-sm font-medium">{scope.name}</code>
                  <span className="text-xs text-muted-foreground">
                    {scope.usedFor}
                  </span>
                </div>
                <p className="mt-0.5 text-xs text-muted-foreground">
                  {scope.description}
                </p>
              </div>
            ))}
          </div>
        </AccordionContent>
      </AccordionItem>
    </Accordion>
  );
}
